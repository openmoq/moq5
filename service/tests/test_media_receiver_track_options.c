#include <moq/media_receiver.h>
#include <moq/subscriber.h>
#include <moq/sim.h>
#include "test_support.h"
#include "test_alloc_support.h"

/* The call-boundary observer forwards to the real facade; it owns no product
 * symbol and does not replace protocol processing with a fake result. */
static moq_result_t observed_subscribe(moq_subscriber_t *,
    const moq_sub_track_cfg_t *, uint64_t, moq_sub_track_t **);
#define moq_sub_subscribe observed_subscribe
#define MOQ_MEDIA_RECEIVER_TESTING 1
#include "../src/media_receiver.c"
#undef moq_sub_subscribe

typedef struct old_track_cfg {
    uint32_t struct_size;
    moq_media_start_mode_t start;
    bool has_priority;
    uint8_t priority;
} old_track_cfg_t;

/* Independent extension declaration also runs against the pre-extension
 * implementation, which must fail the behavioral assertions below. */
typedef struct options_cfg {
    uint32_t struct_size;
    moq_media_start_mode_t start;
    bool has_priority;
    uint8_t priority;
    uint8_t reserved_sub[2];
    uint32_t mpeg2ts_program_number;
    const moq_location_filter_cfg_t *location;
    const moq_fill_cfg_t *fill;
    const uint64_t *object_delivery_timeout_ms;
    const uint64_t *subgroup_delivery_timeout_ms;
    const bool *forward;
} options_cfg_t;

/* The extent a caller had before the delivery timeouts were appended. */
#define OPTIONS_BEFORE_TIMEOUTS offsetof(options_cfg_t, object_delivery_timeout_ms)

_Static_assert(MOQ_MEDIA_RECEIVER_TRACK_SUBSCRIBE_CFG_V0_SIZE == sizeof(old_track_cfg_t),
               "validation uses the independent old extent");
_Static_assert(offsetof(moq_media_receiver_track_subscribe_cfg_t, location) >= sizeof(old_track_cfg_t),
               "location does not reuse old padding");
_Static_assert(sizeof(options_cfg_t) == sizeof(moq_media_receiver_track_subscribe_cfg_t),
               "extension fixture matches the public contract");
_Static_assert(offsetof(options_cfg_t, mpeg2ts_program_number) ==
               offsetof(moq_media_receiver_track_subscribe_cfg_t, mpeg2ts_program_number),
               "independent program number offset");
_Static_assert(offsetof(options_cfg_t, location) == offsetof(moq_media_receiver_track_subscribe_cfg_t, location),
               "independent location offset");
_Static_assert(offsetof(options_cfg_t, fill) == offsetof(moq_media_receiver_track_subscribe_cfg_t, fill),
               "independent fill offset");
_Static_assert(offsetof(options_cfg_t, object_delivery_timeout_ms) ==
               offsetof(moq_media_receiver_track_subscribe_cfg_t, object_delivery_timeout_ms),
               "independent object delivery timeout offset");
_Static_assert(offsetof(options_cfg_t, subgroup_delivery_timeout_ms) ==
               offsetof(moq_media_receiver_track_subscribe_cfg_t, subgroup_delivery_timeout_ms),
               "independent subgroup delivery timeout offset");
_Static_assert(offsetof(options_cfg_t, forward) ==
               offsetof(moq_media_receiver_track_subscribe_cfg_t, forward),
               "independent forward offset");

static int failures;
static unsigned calls;
static moq_subscribe_filter_t seen_filter;
static bool seen_location, seen_fill, seen_fill_location;
static moq_location_filter_cfg_t location_seen, fill_location_seen;
static moq_fill_cfg_t fill_seen;
static const moq_location_filter_cfg_t *caller_location, *caller_fill_location;
static const moq_fill_cfg_t *caller_fill;
static bool seen_object_dt, seen_subgroup_dt;
static uint64_t object_dt_seen, subgroup_dt_seen;
static const uint64_t *caller_object_dt, *caller_subgroup_dt;
static bool seen_forward_set, seen_forward;

static moq_result_t observed_subscribe(moq_subscriber_t *sub,
    const moq_sub_track_cfg_t *cfg, uint64_t now_us, moq_sub_track_t **out)
{
    calls++;
    seen_filter = cfg->filter;
    seen_location = cfg->location != NULL;
    seen_fill = cfg->fill != NULL;
    seen_fill_location = cfg->fill && cfg->fill->location;
    if (seen_location) {
        location_seen = *cfg->location;
        MOQ_TEST_CHECK(cfg->location != caller_location);
    }
    if (seen_fill) {
        fill_seen = *cfg->fill;
        MOQ_TEST_CHECK(cfg->fill != caller_fill);
    }
    if (seen_fill_location) {
        fill_location_seen = *cfg->fill->location;
        MOQ_TEST_CHECK(cfg->fill->location != caller_fill_location);
    }
    seen_object_dt = cfg->object_delivery_timeout_ms != NULL;
    seen_subgroup_dt = cfg->subgroup_delivery_timeout_ms != NULL;
    if (seen_object_dt) {
        object_dt_seen = *cfg->object_delivery_timeout_ms;
        MOQ_TEST_CHECK(cfg->object_delivery_timeout_ms != caller_object_dt);
    }
    if (seen_subgroup_dt) {
        subgroup_dt_seen = *cfg->subgroup_delivery_timeout_ms;
        MOQ_TEST_CHECK(cfg->subgroup_delivery_timeout_ms != caller_subgroup_dt);
    }
    seen_forward_set = cfg->forward != NULL;
    if (seen_forward_set) seen_forward = *cfg->forward;
    return moq_sub_subscribe(sub, cfg, now_us, out);
}

typedef struct fixture {
    test_alloc_state_t ledger;
    moq_simpair_t *pair;
    moq_media_receiver_t *receiver;
    moq_media_track_t *track;
} fixture_t;

static void finish(fixture_t *f)
{
    if (f->receiver) {
        if (f->pair)
            moq_media_receiver_test_destroy_with_session(f->receiver,
                moq_simpair_client(f->pair), moq_simpair_now_us(f->pair));
        else
            moq_media_receiver_test_free(f->receiver);
    }
    if (f->pair) moq_simpair_destroy(f->pair);
    MOQ_TEST_CHECK(f->ledger.balance == 0);
}

static bool start(fixture_t *f, moq_version_t version)
{
    static moq_bytes_t ns[] = {
        { .data = (const uint8_t *)"options", .len = sizeof("options") - 1 }
    };
    static const char catalog[] = "{\"version\":1,\"generatedAt\":1,\"tracks\":["
        "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
        "\"role\":\"video\",\"codec\":\"av01\"}]}";
    memset(f, 0, sizeof(*f));
    calls = 0;
    seen_location = seen_fill = seen_fill_location = false;
    seen_object_dt = seen_subgroup_dt = false;
    caller_location = caller_fill_location = NULL;
    caller_fill = NULL;
    caller_object_dt = caller_subgroup_dt = NULL;
    seen_forward_set = seen_forward = false;
    moq_alloc_t alloc = test_allocator(&f->ledger);
    moq_simpair_cfg_t pair_cfg = MOQ_SIMPAIR_CFG_INIT;
    pair_cfg.alloc = &alloc;
    pair_cfg.version = version;
    pair_cfg.client_send_request_capacity = pair_cfg.server_send_request_capacity = true;
    pair_cfg.client_initial_request_capacity = pair_cfg.server_initial_request_capacity = 32;
    if (moq_simpair_create(&pair_cfg, &f->pair) != MOQ_OK ||
        moq_simpair_start(f->pair) != MOQ_OK ||
        moq_simpair_run_until_quiescent(f->pair, 32, NULL) != MOQ_OK)
        return false;
    moq_session_t *client = moq_simpair_client(f->pair);
    if (moq_session_state(client) != MOQ_SESS_ESTABLISHED ||
        moq_session_version(client) != version) return false;
    moq_session_t *ends[] = {client, moq_simpair_server(f->pair)};
    for (size_t i = 0; i < 2; i++) {
        moq_event_t event;
        for (unsigned j = 0; j < 32 && moq_session_poll_events(ends[i], &event, 1); j++)
            moq_event_cleanup(&event);
    }
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ns, 1};
    cfg.companion_profile = version == MOQ_VERSION_DRAFT_22
        ? MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC : MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
    moq_media_receiver_test_set_alloc(&alloc);
    f->receiver = moq_media_receiver_test_new_cfg(&cfg);
    moq_media_receiver_test_set_alloc(NULL);
    if (!f->receiver) return false;
    f->receiver->transport_version = moq_session_version(client);
    f->receiver->joins_by_fill = version == MOQ_VERSION_DRAFT_22;
    moq_sub_cfg_t sub_cfg;
    moq_sub_cfg_init_sized(&sub_cfg, sizeof(sub_cfg));
    /* The receiver's own acknowledgement hook, as receiver_hook installs it,
     * so an UPDATE's OK ends it in flight (the raise tests send updates). */
    moq_sub_callbacks_init(&sub_cfg.callbacks);
    sub_cfg.callbacks.ctx = f->receiver;
    sub_cfg.on_update_ok = receiver_on_update_ok;
    if (moq_sub_create(client, &alloc, &sub_cfg, &f->receiver->sub) != MOQ_OK)
        return false;
    moq_media_receiver_test_ingest(f->receiver, 0, 0, catalog, sizeof(catalog) - 1);
    moq_media_track_event_t event;
    for (unsigned i = 0; i < 16 &&
         moq_media_receiver_poll_track(f->receiver, &event, sizeof(event)) == MOQ_OK; i++)
        if (event.kind == MOQ_MEDIA_TRACK_ADDED) f->track = event.track;
    return f->track != NULL;
}

static moq_result_t subscribe(fixture_t *f, const options_cfg_t *cfg)
{
    return moq_media_receiver_subscribe_track(f->receiver, f->track,
        (const moq_media_receiver_track_subscribe_cfg_t *)cfg);
}

static void reconcile(fixture_t *f)
{
    receiver_reconcile_subscriptions(f->receiver, moq_simpair_now_us(f->pair));
    MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(f->receiver));
    MOQ_TEST_CHECK(f->track->requested && f->track->sub_track != NULL);
}

static void test_copy_and_selection(void)
{
    fixture_t f;
    bool ready = start(&f, MOQ_VERSION_DRAFT_22);
    MOQ_TEST_CHECK(ready);
    if (!ready) { finish(&f); return; }
    moq_location_filter_cfg_t location, fill_location;
    moq_location_filter_cfg_init(&location);
    location.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    location.start_group = 3; location.start_object = 7;
    moq_location_filter_cfg_init(&fill_location);
    fill_location.kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP;
    fill_location.relative_group_offset = 1;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.has_group_order = true; fill.group_order = MOQ_GROUP_ORDER_DESCENDING;
    fill.has_fill_timeout = true; fill.fill_timeout_ms = UINT64_MAX;
    fill.has_subscriber_priority = true; fill.subscriber_priority = 17;
    fill.location = &fill_location;
    options_cfg_t cfg = {0};
    cfg.struct_size = sizeof(cfg); cfg.location = &location; cfg.fill = &fill;
    caller_location = &location; caller_fill_location = &fill_location; caller_fill = &fill;
    MOQ_TEST_CHECK(subscribe(&f, &cfg) == MOQ_OK);
    MOQ_TEST_CHECK(calls == 0);
    memset(&location, 0, sizeof(location));
    memset(&fill_location, 0, sizeof(fill_location));
    memset(&fill, 0, sizeof(fill));
    memset(&cfg, 0, sizeof(cfg));
    /* A second enable must not replace the pending first request. */
    MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(f.receiver, f.track, NULL) == MOQ_OK);
    reconcile(&f);
    MOQ_TEST_CHECK(calls == 1 && seen_location && seen_fill && seen_fill_location);
    if (seen_location) {
        MOQ_TEST_CHECK(location_seen.kind == MOQ_LOCATION_FILTER_ABSOLUTE_START);
        MOQ_TEST_CHECK(location_seen.start_group == 3 && location_seen.start_object == 7);
    }
    if (seen_fill) {
        MOQ_TEST_CHECK(fill_seen.has_group_order && fill_seen.group_order == MOQ_GROUP_ORDER_DESCENDING);
        MOQ_TEST_CHECK(fill_seen.has_fill_timeout && fill_seen.fill_timeout_ms == UINT64_MAX);
        MOQ_TEST_CHECK(fill_seen.has_subscriber_priority && fill_seen.subscriber_priority == 17);
    }
    if (seen_fill_location)
        MOQ_TEST_CHECK(fill_location_seen.kind == MOQ_LOCATION_FILTER_RELATIVE_GROUP &&
                       fill_location_seen.relative_group_offset == 1);
    finish(&f);
    MOQ_TEST_PASS("track_options_copied_before_network_reconciliation");
}

static void test_authoritative_location(void)
{
    fixture_t f;
    bool ready = start(&f, MOQ_VERSION_DRAFT_22);
    MOQ_TEST_CHECK(ready);
    if (!ready) { finish(&f); return; }
    moq_location_filter_cfg_t location;
    moq_location_filter_cfg_init(&location);
    location.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    options_cfg_t cfg = {0};
    cfg.struct_size = sizeof(cfg); cfg.location = &location;
    cfg.start = (moq_media_start_mode_t)99;
    moq_result_t rc = subscribe(&f, &cfg);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    if (rc == MOQ_OK) {
        reconcile(&f);
        MOQ_TEST_CHECK(seen_location && location_seen.kind == MOQ_LOCATION_FILTER_NEXT_OBJECT);
        MOQ_TEST_CHECK(!seen_fill);
    }
    finish(&f);
    MOQ_TEST_PASS("track_options_location_supersedes_start_no_implicit_fill");
}

static void test_refusals(void)
{
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    for (size_t v = 0; v < 3; v++) {
        for (unsigned row = 0; row < (v == 2 ? 6u : 1u); row++) {
            fixture_t f;
            bool ready = start(&f, versions[v]);
            MOQ_TEST_CHECK(ready);
            if (!ready) { finish(&f); continue; }
            moq_location_filter_cfg_t location;
            moq_location_filter_cfg_init(&location);
            moq_fill_cfg_t fill;
            moq_fill_cfg_init(&fill);
            options_cfg_t cfg = {0};
            cfg.struct_size = sizeof(cfg); cfg.fill = &fill;
            moq_result_t expected = MOQ_ERR_UNSUPPORTED;
            if (v == 2) {
                expected = row < 2 ? MOQ_ERR_ABI_MISMATCH : MOQ_ERR_INVAL;
                if (row == 0) fill.struct_size = MOQ_FILL_CFG_V0_SIZE - 1;
                if (row == 1) { cfg.location = &location; location.struct_size = MOQ_LOCATION_FILTER_CFG_V0_SIZE - 1; }
                if (row == 2) { cfg.location = &location; location.kind = UINT32_MAX; }
                if (row == 3) { fill.has_group_order = true; fill.group_order = MOQ_GROUP_ORDER_DEFAULT; }
                if (row == 4) { fill.location = &location; location.kind = MOQ_LOCATION_FILTER_GROUP_END; location.start_group = 4; location.end_group = 3; }
                if (row == 5) cfg.start = (moq_media_start_mode_t)99;
            }
            uint64_t generation = f.track->app_gen;
            MOQ_TEST_CHECK(subscribe(&f, &cfg) == expected);
            MOQ_TEST_CHECK(!f.track->desired_subscribed && f.track->app_gen == generation);
            MOQ_TEST_CHECK(!f.track->requested && f.track->sub_track == NULL && calls == 0);
            finish(&f);
        }
    }
    MOQ_TEST_PASS("track_options_refusals_leave_desired_state_unchanged");
}

static void test_old_and_partial(void)
{
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    for (size_t v = 0; v < 3; v++) {
        for (size_t n = sizeof(old_track_cfg_t); n < OPTIONS_BEFORE_TIMEOUTS; n++) {
            fixture_t f;
            bool ready = start(&f, versions[v]);
            MOQ_TEST_CHECK(ready);
            if (!ready) { finish(&f); continue; }
            options_cfg_t cfg = {0};
            cfg.struct_size = (uint32_t)n;
            cfg.start = MOQ_MEDIA_START_NEXT_GROUP;
            /* NULL location and a valid but not wholly covered fill pointer. */
            moq_fill_cfg_t fill;
            moq_fill_cfg_init(&fill);
            cfg.fill = &fill;
            const size_t offset = _Alignof(old_track_cfg_t);
            unsigned char *old_storage = malloc(offset + n);
            MOQ_TEST_CHECK(old_storage != NULL);
            if (!old_storage) { finish(&f); continue; }
            memcpy(old_storage + offset, &cfg, n);
            MOQ_TEST_CHECK(subscribe(&f, (const options_cfg_t *)(old_storage + offset)) == MOQ_OK);
            free(old_storage);
            reconcile(&f);
            MOQ_TEST_CHECK(calls == 1 && !seen_location && !seen_fill);
            MOQ_TEST_CHECK(seen_filter == MOQ_SUBSCRIBE_FILTER_NEXT_GROUP);
            finish(&f);
        }
    }
    MOQ_TEST_PASS("track_options_frozen_floor_and_partial_tails");
}

static void test_semantic_values(void)
{
    const uint32_t kinds[] = {MOQ_LOCATION_FILTER_NONE, MOQ_LOCATION_FILTER_RELATIVE_GROUP,
        MOQ_LOCATION_FILTER_ABSOLUTE_START, MOQ_LOCATION_FILTER_GROUP_END,
        MOQ_LOCATION_FILTER_OBJECT_END, MOQ_LOCATION_FILTER_NEXT_OBJECT};
    for (size_t i = 0; i < sizeof(kinds) / sizeof(*kinds); i++) {
        fixture_t f;
        bool ready = start(&f, MOQ_VERSION_DRAFT_22);
        MOQ_TEST_CHECK(ready);
        if (!ready) { finish(&f); continue; }
        moq_location_filter_cfg_t location;
        moq_location_filter_cfg_init(&location);
        location.kind = kinds[i];
        location.relative_group_offset = UINT64_MAX;
        location.start_group = location.start_object = location.end_group = UINT64_MAX;
        /* Equal group and earlier inclusive end object is a legal empty range. */
        location.end_object = 0;
        moq_fill_cfg_t fill;
        moq_fill_cfg_init(&fill);
        fill.subscriber_priority = UINT8_MAX;
        fill.group_order = UINT32_MAX;
        fill.fill_timeout_ms = UINT64_MAX;
        if (i == 4) { fill.has_fill_timeout = true; fill.fill_timeout_ms = 0; }
        options_cfg_t cfg = {0};
        cfg.struct_size = UINT32_MAX; cfg.location = &location; cfg.fill = &fill;
        MOQ_TEST_CHECK(subscribe(&f, &cfg) == MOQ_OK);
        reconcile(&f);
        MOQ_TEST_CHECK(calls == 1 && seen_location && seen_fill && !seen_fill_location);
        if (seen_location) {
            MOQ_TEST_CHECK(location_seen.kind == kinds[i]);
            MOQ_TEST_CHECK(location_seen.relative_group_offset ==
                           (i == 1 ? UINT64_MAX : 0));
            MOQ_TEST_CHECK(location_seen.start_group == (i >= 2 && i <= 4 ? UINT64_MAX : 0));
            MOQ_TEST_CHECK(location_seen.start_object == (i >= 2 && i <= 4 ? UINT64_MAX : 0));
            MOQ_TEST_CHECK(location_seen.end_group == (i == 3 || i == 4 ? UINT64_MAX : 0));
            MOQ_TEST_CHECK(location_seen.end_object == 0);
        }
        if (seen_fill) {
            MOQ_TEST_CHECK(!fill_seen.has_group_order && fill_seen.group_order == 0);
            MOQ_TEST_CHECK(!fill_seen.has_subscriber_priority && fill_seen.subscriber_priority == 0);
            MOQ_TEST_CHECK(fill_seen.has_fill_timeout == (i == 4) && fill_seen.fill_timeout_ms == 0);
        }
        finish(&f);
    }
    MOQ_TEST_PASS("track_options_semantic_full_width_and_inactive_fields");
}

static void test_legacy_locations(void)
{
    /* D16 uses QUIC varints; D18 uses vi64. Keep the boundary literals
     * independent of the product's value-limit predicate. */
    const uint64_t max62 = UINT64_C(0x3fffffffffffffff);
    const struct {
        uint32_t kind;
        uint64_t offset, group, object, end;
        moq_subscribe_filter_t filter;
        moq_result_t d16_result, d18_result;
    } rows[] = {
        {MOQ_LOCATION_FILTER_NONE, UINT64_MAX, UINT64_MAX, UINT64_MAX, 0,
         MOQ_SUBSCRIBE_FILTER_NONE, MOQ_OK, MOQ_OK},
        {MOQ_LOCATION_FILTER_RELATIVE_GROUP, 0, UINT64_MAX, UINT64_MAX, 0,
         MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, MOQ_OK, MOQ_OK},
        {MOQ_LOCATION_FILTER_NEXT_OBJECT, UINT64_MAX, UINT64_MAX, UINT64_MAX, 0,
         MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, MOQ_OK, MOQ_OK},
        {MOQ_LOCATION_FILTER_ABSOLUTE_START, UINT64_MAX, 3, 7, 0,
         MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, MOQ_OK, MOQ_OK},
        {MOQ_LOCATION_FILTER_GROUP_END, UINT64_MAX, 3, 7, 8,
         MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, MOQ_OK, MOQ_OK},
        {MOQ_LOCATION_FILTER_GROUP_END, UINT64_MAX, 3, 7, 3,
         MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, MOQ_OK, MOQ_OK},
        {MOQ_LOCATION_FILTER_RELATIVE_GROUP, 1, 0, 0, 0,
         MOQ_SUBSCRIBE_FILTER_NONE, MOQ_ERR_UNSUPPORTED, MOQ_ERR_UNSUPPORTED},
        {MOQ_LOCATION_FILTER_OBJECT_END, 0, 3, 7, 8,
         MOQ_SUBSCRIBE_FILTER_NONE, MOQ_ERR_UNSUPPORTED, MOQ_ERR_UNSUPPORTED},
        {MOQ_LOCATION_FILTER_ABSOLUTE_START, 0, max62, max62, 0,
         MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, MOQ_OK, MOQ_OK},
        {MOQ_LOCATION_FILTER_GROUP_END, 0, max62, max62, max62,
         MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, MOQ_OK, MOQ_OK},
        {MOQ_LOCATION_FILTER_ABSOLUTE_START, 0, max62 + 1, 0, 0,
         MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, MOQ_ERR_INVAL, MOQ_OK},
        {MOQ_LOCATION_FILTER_ABSOLUTE_START, 0, 0, max62 + 1, 0,
         MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, MOQ_ERR_INVAL, MOQ_OK},
        {MOQ_LOCATION_FILTER_GROUP_END, 0, 0, 0, max62 + 1,
         MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, MOQ_ERR_INVAL, MOQ_OK},
        {MOQ_LOCATION_FILTER_ABSOLUTE_START, 0, UINT64_MAX, UINT64_MAX, 0,
         MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, MOQ_ERR_INVAL, MOQ_OK},
        {MOQ_LOCATION_FILTER_GROUP_END, 0, 0, 0, UINT64_MAX,
         MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, MOQ_ERR_INVAL, MOQ_OK},
        {MOQ_LOCATION_FILTER_GROUP_END, 0, UINT64_MAX, UINT64_MAX, UINT64_MAX,
         MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, MOQ_ERR_INVAL, MOQ_OK},
    };
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18};
    for (size_t v = 0; v < 2; v++) {
        for (size_t row = 0; row < sizeof(rows) / sizeof(rows[0]); row++) {
            fixture_t f;
            bool ready = start(&f, versions[v]);
            MOQ_TEST_CHECK(ready);
            if (!ready) { finish(&f); continue; }
            moq_location_filter_cfg_t location;
            moq_location_filter_cfg_init(&location);
            location.kind = rows[row].kind;
            location.relative_group_offset = rows[row].offset;
            location.start_group = rows[row].group;
            location.start_object = rows[row].object;
            location.end_group = rows[row].end;
            location.end_object = UINT64_MAX;
            options_cfg_t cfg = {0};
            cfg.struct_size = sizeof(cfg); cfg.location = &location;
            cfg.start = (moq_media_start_mode_t)99;
            uint64_t generation = f.track->app_gen;
            moq_result_t expected = v == 0 ? rows[row].d16_result : rows[row].d18_result;
            moq_result_t rc = subscribe(&f, &cfg);
            if (rc != expected)
                fprintf(stderr, "legacy_location draft %u row %zu: result %d != %d\n",
                        v == 0 ? 16u : 18u, row, (int)rc, (int)expected);
            MOQ_TEST_CHECK(rc == expected);
            if (rc == MOQ_OK && expected == MOQ_OK) {
                memset(&location, 0, sizeof(location));
                reconcile(&f);
                if (moq_media_receiver_is_fatal(f.receiver))
                    fprintf(stderr, "legacy_location draft %u row %zu: reconciliation failed\n",
                            v == 0 ? 16u : 18u, row);
                MOQ_TEST_CHECK(calls == 1 && seen_location && !seen_fill);
                MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(f.pair, 32, NULL) == MOQ_OK);
                moq_session_t *server = moq_simpair_server(f.pair);
                moq_event_t event;
                unsigned requests = 0;
                for (unsigned i = 0; i < 32 && moq_session_poll_events(server, &event, 1); i++) {
                    if (event.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                        const moq_subscribe_request_event_t *request = &event.u.subscribe_request;
                        requests++;
                        MOQ_TEST_CHECK(request->filter == rows[row].filter);
                        if (rows[row].filter == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START ||
                            rows[row].filter == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE) {
                            MOQ_TEST_CHECK(request->start_group == rows[row].group);
                            MOQ_TEST_CHECK(request->start_object == rows[row].object);
                        }
                        if (rows[row].filter == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE)
                            MOQ_TEST_CHECK(request->end_group == rows[row].end);
                    }
                    moq_event_cleanup(&event);
                }
                MOQ_TEST_CHECK(requests == 1);
            } else {
                MOQ_TEST_CHECK(!f.track->desired_subscribed && f.track->app_gen == generation);
                MOQ_TEST_CHECK(!f.track->requested && f.track->sub_track == NULL && calls == 0);
            }
            finish(&f);
        }
    }
    MOQ_TEST_PASS("track_options_legacy_locations_and_profile_widths");
}

/* The SUBSCRIBE the peer receives, or false if none arrived. */
static bool peer_subscribe(fixture_t *f, moq_subscribe_request_event_t *out,
                           moq_subscription_admission_options_t *options, bool *has_options)
{
    bool got = false;
    moq_session_t *server = moq_simpair_server(f->pair);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    moq_event_t event;
    for (unsigned i = 0; i < 64 && moq_session_poll_events(server, &event, 1); i++) {
        if (event.kind == MOQ_EVENT_SUBSCRIBE_REQUEST && !got) {
            *out = event.u.subscribe_request;
            *has_options = out->options != NULL;
            if (*has_options) *options = *out->options;
            got = true;
        }
        moq_event_cleanup(&event);
    }
    return got;
}

static void test_delivery_timeouts(void)
{
    /* Draft 16 carries one shared value, so the two must be equal there. */
    const struct { moq_version_t version; uint64_t object_ms, subgroup_ms, wire_us; } cases[] = {
        {MOQ_VERSION_DRAFT_16, 400, 400, 400000},
        {MOQ_VERSION_DRAFT_18, 400, 900, 400000},
        {MOQ_VERSION_DRAFT_22, 400, 900, 0},
    };
    for (size_t c = 0; c < sizeof(cases) / sizeof(cases[0]); c++) {
        fixture_t f;
        bool ready = start(&f, cases[c].version);
        MOQ_TEST_CHECK(ready);
        if (!ready) { finish(&f); continue; }
        uint64_t object_ms = cases[c].object_ms, subgroup_ms = cases[c].subgroup_ms;
        options_cfg_t cfg = {0};
        cfg.struct_size = sizeof(cfg);
        cfg.object_delivery_timeout_ms = &object_ms;
        cfg.subgroup_delivery_timeout_ms = &subgroup_ms;
        caller_object_dt = &object_ms; caller_subgroup_dt = &subgroup_ms;
        MOQ_TEST_CHECK(subscribe(&f, &cfg) == MOQ_OK);
        /* The values are copied: the caller's storage is free once it returns. */
        object_ms = subgroup_ms = 0;
        memset(&cfg, 0, sizeof(cfg));
        reconcile(&f);
        MOQ_TEST_CHECK(calls == 1 && seen_object_dt && seen_subgroup_dt);
        MOQ_TEST_CHECK(object_dt_seen == cases[c].object_ms &&
                       subgroup_dt_seen == cases[c].subgroup_ms);
        /* Every later SUBSCRIBE the reconcile builds for the track (after a
         * resubscribe, say) starts from the track's desired options. */
        MOQ_TEST_CHECK(f.track->desired_options.has_object_delivery_timeout &&
                       f.track->desired_options.object_delivery_timeout_ms == cases[c].object_ms);
        MOQ_TEST_CHECK(f.track->desired_options.has_subgroup_delivery_timeout &&
                       f.track->desired_options.subgroup_delivery_timeout_ms == cases[c].subgroup_ms);
        moq_subscribe_request_event_t request = {0};
        moq_subscription_admission_options_t options = {0};
        bool has_options = false;
        MOQ_TEST_CHECK(peer_subscribe(&f, &request, &options, &has_options));
        if (cases[c].version == MOQ_VERSION_DRAFT_22) {
            /* Draft 22 surfaces each timeout exactly. */
            MOQ_TEST_CHECK(has_options);
            if (has_options) {
                MOQ_TEST_CHECK(options.flags & MOQ_SUBSCRIPTION_ADMISSION_OBJECT_DELIVERY_TIMEOUT);
                MOQ_TEST_CHECK(options.flags & MOQ_SUBSCRIPTION_ADMISSION_SUBGROUP_DELIVERY_TIMEOUT);
                MOQ_TEST_CHECK(options.object_delivery_timeout_ms == cases[c].object_ms);
                MOQ_TEST_CHECK(options.subgroup_delivery_timeout_ms == cases[c].subgroup_ms);
            }
        } else {
            /* Drafts 16 and 18 surface only the smaller nonzero value, so
             * the peer event cannot show that both were sent; the observer
             * above shows both reached moq_sub_subscribe. */
            MOQ_TEST_CHECK(request.delivery_timeout_us == cases[c].wire_us);
        }
        finish(&f);
    }
    MOQ_TEST_PASS("track_options_delivery_timeouts_reach_the_subscribe");
}

static void test_delivery_timeout_refusals(void)
{
    /* Draft 16's single DELIVERY_TIMEOUT: a zero or two different values
     * fail the call, before any state commits, and the receiver lives on. */
    const struct { uint64_t object_ms, subgroup_ms; bool has_subgroup; } bad[] = {
        {400, 900, true}, {0, 0, false},
    };
    for (size_t c = 0; c < sizeof(bad) / sizeof(bad[0]); c++) {
        fixture_t f;
        bool ready = start(&f, MOQ_VERSION_DRAFT_16);
        MOQ_TEST_CHECK(ready);
        if (!ready) { finish(&f); continue; }
        uint64_t object_ms = bad[c].object_ms, subgroup_ms = bad[c].subgroup_ms;
        options_cfg_t cfg = {0};
        cfg.struct_size = sizeof(cfg);
        cfg.object_delivery_timeout_ms = &object_ms;
        if (bad[c].has_subgroup) cfg.subgroup_delivery_timeout_ms = &subgroup_ms;
        MOQ_TEST_CHECK(subscribe(&f, &cfg) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(!f.track->desired_subscribed);
        receiver_reconcile_subscriptions(f.receiver, moq_simpair_now_us(f.pair));
        MOQ_TEST_CHECK(calls == 0 && !moq_media_receiver_is_fatal(f.receiver));
        finish(&f);
    }
    MOQ_TEST_PASS("track_options_delivery_timeout_d16_refusals");
}

static void test_delivery_timeout_tails(void)
{
    /* Sizes that end inside the delivery-timeout tail (before `forward`). */
    for (size_t n = OPTIONS_BEFORE_TIMEOUTS; n < offsetof(options_cfg_t, forward); n++) {
        fixture_t f;
        bool ready = start(&f, MOQ_VERSION_DRAFT_18);
        MOQ_TEST_CHECK(ready);
        if (!ready) { finish(&f); continue; }
        uint64_t object_ms = 400, subgroup_ms = 400;
        options_cfg_t cfg = {0};
        cfg.struct_size = (uint32_t)n;
        cfg.object_delivery_timeout_ms = &object_ms;
        cfg.subgroup_delivery_timeout_ms = &subgroup_ms;
        MOQ_TEST_CHECK(subscribe(&f, &cfg) == MOQ_OK);
        reconcile(&f);
        /* A field is read only when struct_size covers all of it. */
        bool object_covered = n >= offsetof(options_cfg_t, object_delivery_timeout_ms) +
                                    sizeof(cfg.object_delivery_timeout_ms);
        MOQ_TEST_CHECK(calls == 1 && seen_object_dt == object_covered && !seen_subgroup_dt);
        finish(&f);
    }
    MOQ_TEST_PASS("track_options_delivery_timeout_partial_tails");
}

/* The forward pointer is read only when struct_size covers all of it. */
static void test_forward_tails(void)
{
    for (size_t n = offsetof(options_cfg_t, forward); n <= sizeof(options_cfg_t); n++) {
        fixture_t f;
        bool ready = start(&f, MOQ_VERSION_DRAFT_18);
        MOQ_TEST_CHECK(ready);
        if (!ready) { finish(&f); continue; }
        const bool off = false;
        options_cfg_t cfg = {0};
        cfg.struct_size = (uint32_t)n;
        cfg.forward = &off;
        MOQ_TEST_CHECK(subscribe(&f, &cfg) == MOQ_OK);
        reconcile(&f);
        bool covered = n >= offsetof(options_cfg_t, forward) + sizeof(cfg.forward);
        MOQ_TEST_CHECK(calls == 1 && seen_forward_set == covered);
        MOQ_TEST_CHECK(f.track->passive == covered);
        finish(&f);
    }
    MOQ_TEST_PASS("track_options_forward_partial_tails");
}

/* A passive track (forward = &false) subscribes with Forward 0, stays
 * subscribed while silent for 60 s of reconciliation, raises no track event,
 * and sends no SUBSCRIBE_UPDATE. forward = &true subscribes with Forward 1. */
static void test_passive_forward(moq_version_t version)
{
    for (int passive = 1; passive >= 0; --passive) {
        fixture_t f;
        bool ready = start(&f, version);
        MOQ_TEST_CHECK(ready);
        if (!ready) { finish(&f); continue; }
        const bool fwd = !passive;
        options_cfg_t cfg;
        memset(&cfg, 0, sizeof(cfg));
        cfg.struct_size = sizeof(cfg);
        cfg.forward = &fwd;
        MOQ_TEST_CHECK_EQ_INT(subscribe(&f, &cfg), MOQ_OK);
        reconcile(&f);
        MOQ_TEST_CHECK(seen_forward_set && seen_forward == fwd);
        MOQ_TEST_CHECK(f.track->passive == (bool)passive);
        MOQ_TEST_CHECK(f.track->forward_sent == fwd);

        /* The server sees the SUBSCRIBE with that Forward State and accepts it. */
        moq_session_t *server = moq_simpair_server(f.pair);
        moq_simpair_run_until_quiescent(f.pair, 32, NULL);
        moq_event_t ev;
        bool got = false;
        for (unsigned j = 0; j < 8 && moq_session_poll_events(server, &ev, 1); j++) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                got = true;
                MOQ_TEST_CHECK(ev.u.subscribe_request.forward == fwd);
                moq_accept_subscribe_cfg_t acc;
                moq_accept_subscribe_cfg_init(&acc);
                MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(server,
                    ev.u.subscribe_request.sub, &acc, moq_simpair_now_us(f.pair)), MOQ_OK);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        moq_simpair_run_until_quiescent(f.pair, 32, NULL);

        /* 60 s of silence with the receiver reconciling every second. */
        unsigned events = 0, updates = 0, fill_terminals = 0;
        for (int s = 0; s < 60; ++s) {
            moq_simpair_advance_to(f.pair, moq_simpair_now_us(f.pair) + 1000000);
            /* The endpoint's full receiver pass (receiver_hook): the routed
             * tick, reconciliation, MPEG-TS timers, completions, retirement. */
            moq_media_receiver_test_pump(f.receiver, moq_simpair_client(f.pair),
                                         moq_simpair_now_us(f.pair));
            moq_simpair_run_until_quiescent(f.pair, 32, NULL);
            moq_media_track_event_t te;
            while (moq_media_receiver_poll_track(f.receiver, &te, sizeof(te)) == MOQ_OK) {
                /* Draft 22 joins a live track with a current-group fill; with
                 * nothing published it ends at once (FILL_TERMINAL). A passive
                 * track takes no join fill. */
                if (te.kind == MOQ_MEDIA_TRACK_FILL_TERMINAL) fill_terminals++;
                else events++;
            }
            while (moq_session_poll_events(server, &ev, 1)) {
                if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) updates++;
                moq_event_cleanup(&ev);
            }
        }
        MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(f.receiver));
        MOQ_TEST_CHECK(f.track->requested && f.track->sub_track != NULL);
        MOQ_TEST_CHECK_EQ_INT((int)events, 0);
        MOQ_TEST_CHECK_EQ_INT((int)fill_terminals,
                              (!passive && version == MOQ_VERSION_DRAFT_22) ? 1 : 0);
        MOQ_TEST_CHECK_EQ_INT((int)updates, 0);
        MOQ_TEST_CHECK(f.track->forward_sent == fwd);
        finish(&f);
    }
    MOQ_TEST_PASS(version == MOQ_VERSION_DRAFT_22 ? "track_options_passive_forward_d22"
                                                  : "track_options_passive_forward_d18");
}

/* Accept the SUBSCRIBE the server sees; returns whether one arrived. */
static bool accept_subscribe(fixture_t *f)
{
    moq_session_t *server = moq_simpair_server(f->pair);
    moq_simpair_run_until_quiescent(f->pair, 32, NULL);
    moq_event_t ev;
    bool got = false;
    for (unsigned j = 0; j < 8 && moq_session_poll_events(server, &ev, 1); j++) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            got = true;
            moq_accept_subscribe_cfg_t acc;
            moq_accept_subscribe_cfg_init(&acc);
            MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(server,
                ev.u.subscribe_request.sub, &acc, moq_simpair_now_us(f->pair)), MOQ_OK);
        }
        moq_event_cleanup(&ev);
    }
    moq_simpair_run_until_quiescent(f->pair, 32, NULL);
    return got;
}

/* Pump the receiver `seconds` times one second apart; count the server's
 * SUBSCRIBE_UPDATEDs (with the last Forward State), its FETCH requests, and the
 * receiver's fill terminals. */
typedef struct { unsigned updates, fetches, fill_terminals; bool last_forward; } pumped_t;
static pumped_t pump(fixture_t *f, int seconds)
{
    pumped_t p = {0};
    moq_session_t *server = moq_simpair_server(f->pair);
    for (int s = 0; s < seconds; ++s) {
        moq_simpair_advance_to(f->pair, moq_simpair_now_us(f->pair) + 1000000);
        moq_media_receiver_test_pump(f->receiver, moq_simpair_client(f->pair),
                                     moq_simpair_now_us(f->pair));
        moq_simpair_run_until_quiescent(f->pair, 32, NULL);
        moq_media_track_event_t te;
        while (moq_media_receiver_poll_track(f->receiver, &te, sizeof(te)) == MOQ_OK)
            if (te.kind == MOQ_MEDIA_TRACK_FILL_TERMINAL) p.fill_terminals++;
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1)) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED && ev.u.subscribe_updated.has_forward) {
                p.updates++;
                p.last_forward = ev.u.subscribe_updated.forward;
            }
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED && ev.u.subscribe_updated.answer_owed) {
                /* a relay answers the update; the receiver holds the next one
                 * until it does */
                moq_accept_subscription_update_cfg_t acc;
                moq_accept_subscription_update_cfg_init(&acc);
                acc.update_request_id = ev.u.subscribe_updated.update_request_id;
                MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscription_update(server,
                    ev.u.subscribe_updated.sub, &acc, moq_simpair_now_us(f->pair)), MOQ_OK);
            }
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) p.fetches++;
            moq_event_cleanup(&ev);
        }
    }
    return p;
}

/* A passive track raised sends one SUBSCRIBE_UPDATE with Forward 1 and nothing
 * else (no FETCH, no fill, on drafts 18 and 22); lowering sends Forward 0; a
 * raise and lower before reconciliation send nothing; a repeated raise sends
 * nothing more. */
static void test_raise_lower(moq_version_t version)
{
    fixture_t f;
    bool ready = start(&f, version);
    MOQ_TEST_CHECK(ready);
    if (!ready) { finish(&f); return; }
    const bool off = false;
    options_cfg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.struct_size = sizeof(cfg);
    cfg.forward = &off;
    MOQ_TEST_CHECK_EQ_INT(subscribe(&f, &cfg), MOQ_OK);
    reconcile(&f);
    MOQ_TEST_CHECK(f.track->passive && !f.track->forward_sent);
    MOQ_TEST_CHECK(accept_subscribe(&f));
    pumped_t p = pump(&f, 2);
    MOQ_TEST_CHECK_EQ_INT((int)p.updates, 0);

    MOQ_TEST_CHECK_EQ_INT(moq_media_receiver_set_track_forward(f.receiver, f.track, true), MOQ_OK);
    p = pump(&f, 3);
    MOQ_TEST_CHECK_EQ_INT((int)p.updates, 1);
    MOQ_TEST_CHECK(p.last_forward);
    MOQ_TEST_CHECK_EQ_INT((int)p.fetches, 0);
    MOQ_TEST_CHECK_EQ_INT((int)p.fill_terminals, 0);
    MOQ_TEST_CHECK(f.track->forward_sent);

    MOQ_TEST_CHECK_EQ_INT(moq_media_receiver_set_track_forward(f.receiver, f.track, true), MOQ_OK);
    p = pump(&f, 2);
    MOQ_TEST_CHECK_EQ_INT((int)p.updates, 0);            /* already forwarding */

    MOQ_TEST_CHECK_EQ_INT(moq_media_receiver_set_track_forward(f.receiver, f.track, false), MOQ_OK);
    p = pump(&f, 3);
    MOQ_TEST_CHECK_EQ_INT((int)p.updates, 1);
    MOQ_TEST_CHECK(!p.last_forward);
    MOQ_TEST_CHECK(!f.track->forward_sent);

    /* raise then lower before the executor runs: nothing goes out */
    MOQ_TEST_CHECK_EQ_INT(moq_media_receiver_set_track_forward(f.receiver, f.track, true), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(moq_media_receiver_set_track_forward(f.receiver, f.track, false), MOQ_OK);
    p = pump(&f, 3);
    MOQ_TEST_CHECK_EQ_INT((int)p.updates, 0);
    MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(f.receiver));
    finish(&f);
    MOQ_TEST_PASS(version == MOQ_VERSION_DRAFT_22 ? "track_options_raise_lower_d22"
                                                  : "track_options_raise_lower_d18");
}

/* Refusals: NULL arguments; a forwarding (not passive) track; a disabled one. */
static void test_raise_refusals(void)
{
    MOQ_TEST_CHECK_EQ_INT(moq_media_receiver_set_track_forward(NULL, NULL, true), MOQ_ERR_INVAL);
    fixture_t f;
    bool ready = start(&f, MOQ_VERSION_DRAFT_18);
    MOQ_TEST_CHECK(ready);
    if (!ready) { finish(&f); return; }
    MOQ_TEST_CHECK_EQ_INT(moq_media_receiver_set_track_forward(f.receiver, NULL, true), MOQ_ERR_INVAL);
    /* not subscribed yet */
    MOQ_TEST_CHECK_EQ_INT(moq_media_receiver_set_track_forward(f.receiver, f.track, true),
                          MOQ_ERR_WRONG_STATE);
    const bool on = true;
    options_cfg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.struct_size = sizeof(cfg);
    cfg.forward = &on;
    MOQ_TEST_CHECK_EQ_INT(subscribe(&f, &cfg), MOQ_OK);
    reconcile(&f);
    MOQ_TEST_CHECK_EQ_INT(moq_media_receiver_set_track_forward(f.receiver, f.track, true),
                          MOQ_ERR_WRONG_STATE);     /* a forwarding track */
    MOQ_TEST_CHECK_EQ_INT(moq_media_receiver_unsubscribe_track(f.receiver, f.track), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(moq_media_receiver_set_track_forward(f.receiver, f.track, false),
                          MOQ_ERR_WRONG_STATE);     /* disabled */
    finish(&f);
    MOQ_TEST_PASS("track_options_raise_refusals");
}

int main(void)
{
    test_passive_forward(MOQ_VERSION_DRAFT_18);
    test_passive_forward(MOQ_VERSION_DRAFT_22);
    test_copy_and_selection();
    test_authoritative_location();
    test_refusals();
    test_old_and_partial();
    test_semantic_values();
    test_legacy_locations();
    test_delivery_timeouts();
    test_delivery_timeout_refusals();
    test_delivery_timeout_tails();
    test_forward_tails();
    test_raise_lower(MOQ_VERSION_DRAFT_18);
    test_raise_lower(MOQ_VERSION_DRAFT_22);
    test_raise_refusals();
    printf("receiver_track_options: %d failures\n", failures);
    return failures ? 1 : 0;
}
