#include <moq/media_sender.h>

/* Independent declarations of the complete pre-companion records. */
typedef struct old_sender_cfg {
    uint32_t struct_size;
    const moq_endpoint_cfg_t *endpoint;
    moq_namespace_t namespace_;
    moq_bytes_t catalog_track;
    moq_media_send_backpressure_t backpressure;
    uint64_t block_timeout_us;
    uint32_t queue_max_objects, queue_max_bytes;
    uint32_t pre_ready_max_objects, pre_ready_max_bytes;
    bool validate_cmaf;
    const moq_cmsf_content_protection_t *content_protections;
    size_t content_protection_count;
    moq_media_sender_callbacks_t callbacks;
    bool publish_tracks, drop_without_demand;
    uint64_t catalog_refresh_interval_us;
} old_sender_cfg_t;
typedef struct old_send_object {
    uint32_t struct_size;
    moq_rcbuf_t *payload, *properties;
    bool is_sync, starts_group, ends_group;
    uint64_t decode_time_us, presentation_time_us;
    bool has_capture_time;
    uint64_t capture_time_us;
    bool has_sap_type;
    moq_sap_type_t sap_type;
} old_send_object_t;
typedef struct old_callbacks_v0 {
    uint32_t struct_size;
    void *ctx;
    void (*joined)(void *, moq_media_sender_t *, moq_media_track_t *, size_t);
    void (*left)(void *, moq_media_sender_t *, moq_media_track_t *, size_t);
} old_callbacks_v0_t;

#ifdef __cplusplus
#define PIN static_assert
#define ALIGNOF alignof
#else
#define PIN _Static_assert
#define ALIGNOF _Alignof
#endif
#define PIN_CFG(f) PIN(offsetof(old_sender_cfg_t, f) == offsetof(moq_media_sender_cfg_t, f), "old cfg " #f)
#define PIN_OBJ(f) PIN(offsetof(old_send_object_t, f) == offsetof(moq_media_send_object_t, f), "old object " #f)
PIN_CFG(struct_size); PIN_CFG(endpoint); PIN_CFG(namespace_); PIN_CFG(catalog_track);
PIN_CFG(backpressure); PIN_CFG(block_timeout_us); PIN_CFG(queue_max_objects);
PIN_CFG(queue_max_bytes); PIN_CFG(pre_ready_max_objects); PIN_CFG(pre_ready_max_bytes);
PIN_CFG(validate_cmaf); PIN_CFG(content_protections); PIN_CFG(content_protection_count);
PIN_CFG(callbacks); PIN_CFG(publish_tracks); PIN_CFG(drop_without_demand);
PIN_CFG(catalog_refresh_interval_us);
PIN_OBJ(struct_size); PIN_OBJ(payload); PIN_OBJ(properties); PIN_OBJ(is_sync);
PIN_OBJ(starts_group); PIN_OBJ(ends_group); PIN_OBJ(decode_time_us);
PIN_OBJ(presentation_time_us); PIN_OBJ(has_capture_time); PIN_OBJ(capture_time_us);
PIN_OBJ(has_sap_type); PIN_OBJ(sap_type);
#ifdef MOQ_MEDIA_SEND_OBJECT_V0_SIZE
PIN(MOQ_MEDIA_SEND_OBJECT_V0_SIZE == sizeof(old_send_object_t), "complete old object floor");
PIN(offsetof(moq_media_send_object_t, metadata) == sizeof(old_send_object_t), "metadata never uses old padding");
PIN(offsetof(moq_media_sender_cfg_t, companion_profile) == sizeof(old_sender_cfg_t), "companion never uses old padding");
PIN(ALIGNOF(old_send_object_t) == ALIGNOF(moq_media_send_object_t), "object alignment stable");
PIN(ALIGNOF(old_sender_cfg_t) == ALIGNOF(moq_media_sender_cfg_t), "cfg alignment stable");
PIN(sizeof(moq_media_send_object_t) ==
    ((sizeof(old_send_object_t) + sizeof(void *) + ALIGNOF(old_send_object_t) - 1) /
     ALIGNOF(old_send_object_t)) * ALIGNOF(old_send_object_t), "full object extent");
PIN(sizeof(moq_media_sender_cfg_t) == sizeof(old_sender_cfg_t) + ALIGNOF(old_sender_cfg_t), "full cfg extent");
#if UINTPTR_MAX == UINT64_MAX
PIN(sizeof(old_sender_cfg_t) == 176 && sizeof(moq_media_sender_cfg_t) == 184, "LP64 cfg pin");
PIN(sizeof(old_send_object_t) == 72 && sizeof(moq_media_send_object_t) == 80, "LP64 object pin");
#elif defined(__arm__)
PIN(sizeof(old_sender_cfg_t) == 112 && sizeof(moq_media_sender_cfg_t) == 120, "ARM cfg pin");
PIN(sizeof(old_send_object_t) == 56 && sizeof(moq_media_send_object_t) == 64, "ARM object pin");
#elif defined(__i386__)
PIN(sizeof(old_sender_cfg_t) == 104 && sizeof(moq_media_sender_cfg_t) == 108, "i686 cfg pin");
PIN(sizeof(old_send_object_t) == (sizeof(moq_sap_type_t) == 1 ? 48 : 52), "i686 old object pin");
PIN(sizeof(moq_media_send_object_t) == (sizeof(moq_sap_type_t) == 1 ? 52 : 56), "i686 full object pin");
#endif
#endif

#ifndef SENDER_COMPANION_LAYOUT_ONLY
#include <moq/sim.h>
#include <moq/wire.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>

moq_result_t moq_media_sender_test_construct(const moq_media_sender_cfg_t *, moq_media_sender_t **);
void moq_media_sender_test_free(moq_media_sender_t *);
void moq_media_sender_test_pump(moq_media_sender_t *, moq_session_t *, uint64_t);
void moq_media_sender_test_set_alloc(const moq_alloc_t *);
void moq_media_sender_test_set_clock_hook(int (*)(struct timespec *, void *), void *);
void moq_media_sender_test_set_cond_wait_hook(int (*)(const struct timespec *, void *), void *);
void moq_media_sender_test_set_ep(moq_media_sender_t *, moq_endpoint_t *);
void moq_media_sender_test_set_clock_ms(moq_media_sender_t *, bool, uint64_t);
uint64_t moq_media_sender_test_catalog_group(const moq_media_sender_t *);
moq_media_companion_profile_t moq_media_sender_test_companion(const moq_media_sender_t *);
moq_result_t moq_media_sender_test_build_catalog(moq_media_sender_t *, moq_rcbuf_t **);
/* Scripted wallclock for the Group ID base, read while the sender is
 * constructed: g_clock_rc != 0 fails the read; otherwise g_clock_ts. */
static int g_clock_rc;
static struct timespec g_clock_ts;
static int scripted_clock(struct timespec *out, void *ctx)
{
    (void)ctx;
    if (g_clock_rc == 0) *out = g_clock_ts;
    return g_clock_rc;
}
static struct timespec ms_clock(uint64_t ms)
{
    return (struct timespec){.tv_sec=(time_t)(ms / 1000u),
                             .tv_nsec=(long)(ms % 1000u) * 1000000L};
}
moq_endpoint_t *moq_endpoint_test_make_bare(void);
void moq_endpoint_test_set_offer(moq_endpoint_t *, const moq_version_t *, size_t);
void moq_endpoint_test_free_bare(moq_endpoint_t *);

static unsigned failures, rows;
static const char *label;
#define CHECK(x) do { if (!(x)) { ++failures; fprintf(stderr,"FAIL[%s]:%d: %s\n", label,__LINE__,#x); } } while (0)
#define REQUIRE(x) do { if (!(x)) { fprintf(stderr,"FIXTURE[%s]:%d: %s\n",label,__LINE__,#x); exit(2); } } while (0)

typedef struct allocation { size_t live, calls, fail, frees; } allocation_t;
static void *allocate(size_t n, void *ctx)
{
    allocation_t *a = ctx;
    if (++a->calls == a->fail) return NULL;
    void *p = malloc(n);
    if (p) ++a->live;
    return p;
}
static void release(void *p, size_t n, void *ctx)
{
    (void)n;
    if (p) {
        allocation_t *a = ctx;
        --a->live; ++a->frees; free(p);
    }
}
static void *resize(void *p, size_t old, size_t n, void *ctx)
{
    allocation_t *a = ctx;
    (void)old;
    if (!p) return allocate(n, ctx);
    if (!n) { release(p, old, ctx); return NULL; }
    if (++a->calls == a->fail) return NULL;
    return realloc(p, n);
}
static moq_bytes_t ns_part = {(const uint8_t *)"companion", 9};
static old_sender_cfg_t *config(size_t tail, uint32_t profile, uint32_t budget)
{
    size_t n = sizeof(old_sender_cfg_t) + tail;
    old_sender_cfg_t *cfg = calloc(1, n);
    REQUIRE(cfg);
    moq_media_sender_cfg_init_live_sized((moq_media_sender_cfg_t *)cfg,
                                        sizeof(old_sender_cfg_t));
    cfg->struct_size = (uint32_t)n;
    cfg->namespace_ = (moq_namespace_t){&ns_part, 1};
    cfg->backpressure = MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK;
    cfg->validate_cmaf = false;
    cfg->pre_ready_max_bytes = cfg->queue_max_bytes = budget;
    if (tail) memcpy((uint8_t *)cfg + sizeof(old_sender_cfg_t), &profile,
                      tail < sizeof(profile) ? tail : sizeof(profile));
    return cfg;
}
static old_send_object_t *object(size_t tail, moq_media_metadata_t *meta)
{
    size_t n = sizeof(old_send_object_t) + tail;
    old_send_object_t *o = calloc(1, n);
    REQUIRE(o);
    o->struct_size = (uint32_t)n;
    o->is_sync = o->starts_group = o->ends_group = true;
    o->presentation_time_us = 17;
    o->has_capture_time = true; o->capture_time_us = 99;
    if (tail) memcpy((uint8_t *)o + sizeof(old_send_object_t), &meta,
                     tail < sizeof(meta) ? tail : sizeof(meta));
    return o;
}
static void metadata(moq_media_metadata_t *m, bool application, uint64_t ticks, uint64_t scale)
{
    memset(m, 0, sizeof(*m));
    m->struct_size = sizeof(*m);
    moq_loc_metadata_init_sized(&m->loc, sizeof(m->loc));
    m->loc.present = MOQ_LOC_METADATA_HAS_TIMESTAMP;
    m->loc.timestamp = ticks;
    if (application) {
        m->loc.present |= MOQ_LOC_METADATA_HAS_TIMESCALE;
        m->loc.timescale = scale;
    }
}
typedef struct fixture {
    allocation_t memory;
    moq_alloc_t alloc;
    moq_media_sender_t *sender;
    moq_media_track_t *track;
    moq_simpair_t *pair;
    moq_session_t *local, *peer;
    moq_subscription_t subscription;
} fixture_t;
static void drain_events(moq_session_t *s)
{
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1)) moq_event_cleanup(&e);
}
static bool start(fixture_t *f, old_sender_cfg_t *cfg, moq_version_t version,
                  moq_media_packaging_t packaging)
{
#define START_STEP(expr) do { bool ok = (expr); CHECK(ok); if (!ok) return false; } while (0)
    f->alloc = (moq_alloc_t){.ctx=&f->memory,.alloc=allocate,.realloc=resize,.free=release};
    moq_media_sender_test_set_alloc(&f->alloc);
    START_STEP(moq_media_sender_test_construct((const moq_media_sender_cfg_t *)cfg, &f->sender) == MOQ_OK);
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = MOQ_BYTES_LITERAL("v"); tc.codec = MOQ_BYTES_LITERAL("av01");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO; tc.packaging = packaging; tc.bitrate = 1000;
    /* CMSF-01 3.1: a CMAF track carries its initialization (opaque here). */
    static const uint8_t cmaf_init[] = {0x00,0x00,0x00,0x18};
    if (packaging == MOQ_MEDIA_PACKAGING_CMAF)
        tc.init_data = (moq_bytes_t){cmaf_init, sizeof(cmaf_init)};
    START_STEP(moq_media_sender_add_track(f->sender, &tc, &f->track) == MOQ_OK);
    moq_simpair_cfg_t pc = MOQ_SIMPAIR_CFG_INIT;
    pc.alloc = moq_alloc_default(); pc.version = version;
    pc.client_send_request_capacity = pc.server_send_request_capacity = true;
    pc.client_initial_request_capacity = pc.server_initial_request_capacity = 32;
    START_STEP(moq_simpair_create(&pc, &f->pair) == MOQ_OK);
    START_STEP(moq_simpair_start(f->pair) == MOQ_OK);
    START_STEP(moq_simpair_run_until_quiescent(f->pair, 16, NULL) == MOQ_OK);
    f->local = moq_simpair_client(f->pair); f->peer = moq_simpair_server(f->pair);
    drain_events(f->local); drain_events(f->peer);
    moq_media_sender_test_pump(f->sender, f->local, 1000);
#undef START_STEP
    return true;
}
static void finish(fixture_t *f)
{
    moq_media_sender_test_free(f->sender);
    moq_media_sender_test_set_alloc(NULL);
    moq_simpair_destroy(f->pair);
    CHECK(f->memory.live == 0);
}
static moq_media_sender_stats_t stats(fixture_t *f)
{
    moq_media_sender_stats_t st;
    REQUIRE(moq_media_sender_get_stats(f->sender, &st, sizeof(st)) == MOQ_OK);
    return st;
}
static moq_result_t write_object(fixture_t *f, old_send_object_t *o)
{
    const uint8_t data[] = {0xa1,0xb2,0xc3,0xd4};
    REQUIRE(moq_rcbuf_create(&f->alloc, data, sizeof(data), &o->payload) == MOQ_OK);
    moq_result_t rc = moq_media_sender_write(f->sender, f->track, (const moq_media_send_object_t *)o);
    if (rc != MOQ_OK) moq_rcbuf_decref(o->payload);
    return rc;
}
/* A failed fixture prerequisite ends dependent work, but leaves teardown to
 * the caller so queued references and allocator accounting are still checked. */
static bool arm_peer(fixture_t *f)
{
#define PEER_STEP(expr) do { bool ok = (expr); CHECK(ok); if (!ok) return false; } while (0)
    PEER_STEP(moq_simpair_run_until_quiescent(f->pair, 16, NULL) == MOQ_OK);
    moq_event_t e;
    while (moq_session_poll_events(f->peer, &e, 1)) {
        if (e.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            moq_accept_namespace_cfg_t ac; moq_accept_namespace_cfg_init(&ac);
            moq_result_t rc = moq_session_accept_namespace(f->peer, e.u.namespace_published.ann, &ac, 1001);
            moq_event_cleanup(&e);
            PEER_STEP(rc == MOQ_OK);
            continue;
        } else {
            CHECK(false);
            moq_event_cleanup(&e);
            return false;
        }
        moq_event_cleanup(&e);
    }
    PEER_STEP(moq_simpair_run_until_quiescent(f->pair, 16, NULL) == MOQ_OK);
    moq_media_sender_test_pump(f->sender, f->local, 1002);
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){&ns_part,1}; sc.track_name = MOQ_BYTES_LITERAL("v");
    PEER_STEP(moq_session_subscribe(f->peer, &sc, 1003, &f->subscription) == MOQ_OK);
    PEER_STEP(moq_simpair_run_until_quiescent(f->pair, 16, NULL) == MOQ_OK);
    moq_media_sender_test_pump(f->sender, f->local, 1004);
    PEER_STEP(moq_simpair_run_until_quiescent(f->pair, 16, NULL) == MOQ_OK);
#undef PEER_STEP
    return true;
}
static void expect_wire(fixture_t *f, const uint8_t *properties, size_t n)
{
    if (!arm_peer(f)) return;
    moq_event_t e;
    unsigned seen = 0;
    while (moq_session_poll_events(f->peer, &e, 1)) {
        if (e.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            ++seen;
            CHECK(moq_subscription_eq(e.u.object_received.sub, f->subscription));
            if (moq_rcbuf_len(e.u.object_received.properties) != n) {
                fprintf(stderr,"WIRE expected=%zu actual=%zu:",n,moq_rcbuf_len(e.u.object_received.properties));
                for (size_t i = 0; i < moq_rcbuf_len(e.u.object_received.properties); ++i)
                    fprintf(stderr," %02x",moq_rcbuf_data(e.u.object_received.properties)[i]);
                fputc('\n',stderr);
            }
            CHECK(moq_rcbuf_len(e.u.object_received.properties) == n);
            if (moq_rcbuf_len(e.u.object_received.properties) == n && n)
                CHECK(memcmp(moq_rcbuf_data(e.u.object_received.properties), properties, n) == 0);
            const uint8_t data[] = {0xa1,0xb2,0xc3,0xd4};
            CHECK(moq_rcbuf_len(e.u.object_received.payload) == sizeof(data));
            if (moq_rcbuf_len(e.u.object_received.payload) == sizeof(data))
                CHECK(memcmp(moq_rcbuf_data(e.u.object_received.payload), data, sizeof(data)) == 0);
        }
        moq_event_cleanup(&e);
    }
    CHECK(seen == 1);
    CHECK(!moq_media_sender_is_fatal(f->sender));
    CHECK(moq_session_state(f->local) == MOQ_SESS_ESTABLISHED);
    CHECK(stats(f).objects_sent == 1 && stats(f).objects_queued == 0 && stats(f).bytes_queued == 0);
}
static void configuration_controls(void)
{
    label = "config-prefix-future-unknown-and-initializer-canaries"; ++rows;
    /* A partially covered field is absent (AUTO); a covered unknown value
     * is refused. */
    for (size_t tail = 0; tail < 9; ++tail) {
        old_sender_cfg_t *cfg = config(tail, tail < 4 ? UINT32_MAX :
                                             MOQ_MEDIA_COMPANION_AUTO + 1u, 1024);
        moq_media_sender_t *s = NULL;
        moq_result_t rc = moq_media_sender_test_construct((const moq_media_sender_cfg_t *)cfg, &s);
        CHECK(rc == (tail < 4 ? MOQ_OK : MOQ_ERR_INVAL));
        if (s) CHECK(moq_media_sender_test_companion(s) == MOQ_MEDIA_COMPANION_AUTO);
        if (s) moq_media_sender_test_free(s);
        free(cfg);
    }
    {
        old_sender_cfg_t *cfg = config(8, UINT32_MAX, 1024);
        moq_media_sender_t *s = NULL;
        CHECK(moq_media_sender_test_construct((const moq_media_sender_cfg_t *)cfg, &s) == MOQ_ERR_INVAL && !s);
        free(cfg);
    }
    /* Every known value is kept as configured until a session is observed. */
    const uint32_t known[] = {MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
        MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC, MOQ_MEDIA_COMPANION_AUTO};
    for (size_t k = 0; k < sizeof(known) / sizeof(*known); ++k) {
        old_sender_cfg_t *cfg = config(4, known[k], 1024);
        moq_media_sender_t *s = NULL;
        CHECK(moq_media_sender_test_construct((const moq_media_sender_cfg_t *)cfg, &s) == MOQ_OK);
        if (s) {
            CHECK(moq_media_sender_test_companion(s) == known[k]);
            moq_media_sender_test_free(s);
        }
        free(cfg);
    }
    old_sender_cfg_t *cfg = config(4, 0, 1024);
    cfg->struct_size = (uint32_t)offsetof(moq_media_sender_cfg_t, callbacks) - 1;
    moq_media_sender_t *s = NULL;
    CHECK(moq_media_sender_test_construct((const moq_media_sender_cfg_t *)cfg, &s) == MOQ_ERR_INVAL);
    free(cfg);
    const size_t cap = sizeof(moq_media_sender_cfg_t) + 16;
    uint8_t *p = malloc(cap); REQUIRE(p);
    void (*init[])(moq_media_sender_cfg_t *) = {
        moq_media_sender_cfg_init, moq_media_sender_cfg_init_live, moq_media_sender_cfg_init_lossless};
    for (size_t i = 0; i < 3; ++i) {
        memset(p, 0xa5, cap); init[i]((moq_media_sender_cfg_t *)p);
        uint32_t stamp; memcpy(&stamp,p,sizeof(stamp));
        CHECK(stamp == offsetof(old_sender_cfg_t, callbacks) + sizeof(old_callbacks_v0_t));
        for (size_t j = stamp; j < cap; ++j) CHECK(p[j] == 0xa5);
    }
    for (size_t n = 0; n <= cap; ++n) {
        memset(p, 0xa5, cap);
        moq_media_sender_cfg_init_live_sized((moq_media_sender_cfg_t *)p,n);
        size_t written = n < 4 ? 0 : n < sizeof(moq_media_sender_cfg_t) ? n : sizeof(moq_media_sender_cfg_t);
        if (written) {
            uint32_t stamp; memcpy(&stamp,p,sizeof(stamp)); CHECK(stamp == written);
        }
        /* The AUTO default is stamped only when the field is wholly covered. */
        const size_t at = offsetof(moq_media_sender_cfg_t, companion_profile);
        if (written >= at + sizeof(uint32_t)) {
            uint32_t companion; memcpy(&companion, p + at, sizeof(companion));
            CHECK(companion == MOQ_MEDIA_COMPANION_AUTO);
        }
        for (size_t j = written; j < cap; ++j) CHECK(p[j] == 0xa5);
    }
    free(p);
}
static void mismatch(moq_version_t version, size_t tail, uint32_t profile)
{
    label = "negotiated-companion-mismatch-without-endpoint-fatal"; ++rows;
    old_sender_cfg_t *cfg = config(tail,profile,1024);
    fixture_t f = {0}; bool started = start(&f,cfg,version,MOQ_MEDIA_PACKAGING_RAW); free(cfg);
    if (!started) { finish(&f); return; }
    CHECK(stats(&f).last_error == MOQ_ERR_UNSUPPORTED);
    CHECK(!moq_media_sender_is_ready(f.sender) && !moq_media_sender_is_fatal(f.sender));
    CHECK(moq_session_state(f.local) == MOQ_SESS_ESTABLISHED);
    moq_action_t a;
    size_t actions = moq_session_poll_actions(f.local,&a,1);
    CHECK(actions == 0);
    if (actions) moq_action_cleanup(&a);
    moq_media_metadata_t m; metadata(&m,false,7,0);
    old_send_object_t *o = object(sizeof(void *), &m);
    CHECK(write_object(&f,o) == MOQ_ERR_UNSUPPORTED); free(o);
    CHECK(stats(&f).objects_written == 0 && stats(&f).bytes_queued == 0);
    finish(&f);
}
static void wire_control(moq_version_t version, size_t cfg_tail, uint32_t profile,
                        moq_media_packaging_t packaging, bool application)
{
    bool d22 = cfg_tail >= sizeof(profile) && profile == MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
    label = d22 ? packaging == MOQ_MEDIA_PACKAGING_CMAF ? "d22-cmaf-old-object-literal-passthrough" :
        "d22-authoritative-owned-metadata-literal-drain" : "legacy-raw-cmaf-unchanged"; ++rows;
    old_sender_cfg_t *cfg = config(cfg_tail,profile,1024);
    fixture_t f = {0}; bool started = start(&f,cfg,version,packaging); free(cfg);
    if (!started) { finish(&f); return; }
    moq_media_metadata_t m; metadata(&m,application,7,1000000);
    uint8_t config_bytes[] = {0x12,0x34};
    if (d22) {
        m.loc.present |= MOQ_LOC_METADATA_HAS_MARKING | MOQ_LOC_METADATA_HAS_VIDEO_CONFIG | MOQ_LOC_METADATA_HAS_AUDIO_CONFIG;
        m.loc.marking_len = 4; m.loc.marking[0] = 0x20; m.loc.marking[1] = 0x80;
        m.loc.marking[2] = 0x01; m.loc.marking[3] = 0x02;
        m.loc.video_config = (moq_bytes_t){config_bytes,sizeof(config_bytes)};
        m.loc.audio_config = (moq_bytes_t){NULL,0};
    }
    old_send_object_t *o = object(d22 && packaging == MOQ_MEDIA_PACKAGING_RAW ? sizeof(void *) + 8 : 0,&m);
    const uint8_t cmaf_props[] = {0x78,1};
    moq_rcbuf_t *props_observer = NULL;
    if (packaging == MOQ_MEDIA_PACKAGING_CMAF) {
        moq_result_t rc = moq_rcbuf_create(&f.alloc,cmaf_props,sizeof(cmaf_props),&o->properties);
        CHECK(rc == MOQ_OK);
        if (rc != MOQ_OK) { free(o); finish(&f); return; }
        props_observer = moq_rcbuf_incref(o->properties);
        CHECK(o->struct_size == sizeof(old_send_object_t));
    }
    const uint8_t unix_props[] = {9,4,0x20,0x80,1,2,4,2,0x12,0x34,2,0,1,7};
    const uint8_t app_props[] = {8,0xcf,0x42,0x40,1,4,0x20,0x80,1,2,4,2,0x12,0x34,2,0,1,7};
    const uint8_t loc16[] = {2,0x40,99,2,0x20};
    const uint8_t loc18[] = {2,99,2,0x20};
    const uint8_t *oracle = packaging == MOQ_MEDIA_PACKAGING_CMAF ? cmaf_props :
        d22 ? (application ? app_props : unix_props) : version == MOQ_VERSION_DRAFT_16 ? loc16 : loc18;
    size_t n = packaging == MOQ_MEDIA_PACKAGING_CMAF ? sizeof(cmaf_props) :
        d22 ? (application ? sizeof(app_props) : sizeof(unix_props)) :
        version == MOQ_VERSION_DRAFT_16 ? sizeof(loc16) : sizeof(loc18);
    moq_result_t rc = write_object(&f,o);
    CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK && o->properties) moq_rcbuf_decref(o->properties);
    if (rc == MOQ_OK) {
        if (props_observer) CHECK(moq_rcbuf_refcount(props_observer) == 2);
        CHECK(stats(&f).bytes_queued == 4 + (d22 || packaging == MOQ_MEDIA_PACKAGING_CMAF ? n : 0));
        memset(&m,0xa5,sizeof(m)); memset(config_bytes,0xa5,sizeof(config_bytes));
        free(o); o = NULL;
        expect_wire(&f,oracle,n);
        if (props_observer) CHECK(moq_rcbuf_refcount(props_observer) == 1);
    }
    moq_rcbuf_decref(props_observer);
    free(o); finish(&f);
}
static void refusal(unsigned kind)
{
    label = "d22-refusal-before-payload-transfer"; ++rows;
    old_sender_cfg_t *cfg = config(4,1,kind == 10 ? 5 : 1024);
    fixture_t f = {0}; bool started = start(&f,cfg,MOQ_VERSION_DRAFT_22,MOQ_MEDIA_PACKAGING_RAW); free(cfg);
    if (!started) { finish(&f); return; }
    moq_media_metadata_t m; metadata(&m,true,7,1000000);
    size_t tail = sizeof(void *);
    switch (kind) {
    case 0: tail = 0; break;
    case 1: tail--; break;
    case 2: m.struct_size = MOQ_MEDIA_METADATA_V0_SIZE - 1; break;
    case 3: m.loc.struct_size = MOQ_LOC_METADATA_V0_SIZE - 1; break;
    case 4: m.loc.present = 0; break;
    case 5: m.loc.timescale = 0; break;
    case 6: m.loc.timestamp = UINT64_MAX; m.loc.timescale = 1; break;
    case 7: m.loc.present |= MOQ_LOC_METADATA_HAS_VIDEO_CONFIG; m.loc.video_config = (moq_bytes_t){NULL,1}; break;
    case 8: m.loc.present |= MOQ_LOC_METADATA_HAS_MARKING; m.loc.marking_len = 5; break;
    case 9: m.loc.reserved[0] = 1; break;
    case 11: m.loc.present |= UINT32_C(0x80000000); break;
    case 12: break;
    case 14: m.loc.timestamp = (UINT64_MAX / 1000000u) * 3 + 2; m.loc.timescale = 3; break;
    default: break;
    }
    old_send_object_t *o = object(tail,kind == 12 ? NULL : &m);
    if (kind == 13) o->struct_size = (uint32_t)sizeof(old_send_object_t) - 1;
    moq_result_t rc = write_object(&f,o);
    CHECK(rc == (kind == 10 ? MOQ_ERR_WOULD_BLOCK : MOQ_ERR_INVAL));
    CHECK(stats(&f).objects_written == 0 && stats(&f).objects_queued == 0 && stats(&f).bytes_queued == 0);
    CHECK(!moq_media_sender_is_fatal(f.sender));
    free(o); finish(&f);
}
static void allocation_refusal(bool large, unsigned offset)
{
    label = "generated-property-allocation-refusal-retry"; ++rows;
    old_sender_cfg_t *cfg = config(4,1,1024);
    fixture_t f = {0}; bool started = start(&f,cfg,MOQ_VERSION_DRAFT_22,MOQ_MEDIA_PACKAGING_RAW); free(cfg);
    if (!started) { finish(&f); return; }
    moq_media_metadata_t m; metadata(&m,false,7,0);
    uint8_t config_bytes[300]; memset(config_bytes,0x5a,sizeof(config_bytes));
    if (large) {
        m.loc.present |= MOQ_LOC_METADATA_HAS_VIDEO_CONFIG;
        m.loc.video_config = (moq_bytes_t){config_bytes,sizeof(config_bytes)};
    }
    old_send_object_t *o = object(sizeof(void *),&m);
    const uint8_t data[] = {0xa1,0xb2,0xc3,0xd4};
    REQUIRE(moq_rcbuf_create(&f.alloc,data,sizeof(data),&o->payload) == MOQ_OK);
    size_t live = f.memory.live;
    f.memory.fail = f.memory.calls + offset;
    moq_result_t rc = moq_media_sender_write(f.sender,f.track, (const moq_media_send_object_t *)o);
    CHECK(rc == MOQ_ERR_NOMEM);
    CHECK(f.memory.calls == f.memory.fail);
    CHECK(f.memory.live == live);
    CHECK(stats(&f).objects_written == 0 && stats(&f).bytes_queued == 0);
    f.memory.fail = 0;
    if (rc != MOQ_OK) {
        rc = moq_media_sender_write(f.sender,f.track, (const moq_media_send_object_t *)o);
        CHECK(rc == MOQ_OK);
        if (rc != MOQ_OK) moq_rcbuf_decref(o->payload);
    }
    if (rc == MOQ_OK) {
        const uint8_t small[] = {0x10,7};
        uint8_t oracle[305] = {0x0d,0x81,0x2c};
        memset(oracle+3,0x5a,300); oracle[303] = 3; oracle[304] = 7;
        memset(&m,0xa5,sizeof(m)); memset(config_bytes,0xa5,sizeof(config_bytes));
        expect_wire(&f,large ? oracle : small,large ? sizeof(oracle) : sizeof(small));
    }
    free(o); finish(&f);
}

static int timeout_clock(struct timespec *out, void *ctx)
{
    unsigned *calls = ctx;
    REQUIRE(*calls < 2);
    *out = (struct timespec){.tv_sec=(time_t)(*calls),.tv_nsec=0};
    ++*calls;
    return 0;
}
static void timeout_refusal(void)
{
    label = "generated-properties-released-on-scripted-timeout"; ++rows;
    old_sender_cfg_t *cfg = config(4,1,5);
    cfg->backpressure = MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT;
    cfg->block_timeout_us = 1;
    fixture_t f = {0}; bool started = start(&f,cfg,MOQ_VERSION_DRAFT_22,MOQ_MEDIA_PACKAGING_RAW); free(cfg);
    if (!started) { finish(&f); return; }
    moq_media_metadata_t m; metadata(&m,false,7,0);
    old_send_object_t *o = object(sizeof(void *),&m);
    unsigned calls = 0;
    moq_media_sender_test_set_clock_hook(timeout_clock,&calls);
    CHECK(write_object(&f,o) == MOQ_ERR_WOULD_BLOCK);
    moq_media_sender_test_set_clock_hook(NULL,NULL);
    CHECK(calls == 2);
    CHECK(stats(&f).objects_written == 0 && stats(&f).bytes_queued == 0);
    free(o); finish(&f);
}

static void time_endpoints(unsigned kind)
{
    label = "explicit-zero-and-full-width-usable-time"; ++rows;
    old_sender_cfg_t *cfg = config(4,1,1024);
    fixture_t f = {0}; bool started = start(&f,cfg,MOQ_VERSION_DRAFT_22,MOQ_MEDIA_PACKAGING_RAW); free(cfg);
    if (!started) { finish(&f); return; }
    moq_media_metadata_t m;
    metadata(&m,kind >= 2,kind ? UINT64_MAX : 0,kind == 2 ? 1000000 : UINT64_MAX);
    uint8_t oracle[24]; size_t n = 0;
    if (kind >= 2) {
        oracle[n++] = 8;
        if (kind == 2) { oracle[n++] = 0xcf; oracle[n++] = 0x42; oracle[n++] = 0x40; }
        else for (unsigned i = 0; i < 9; ++i) oracle[n++] = 0xff;
    }
    oracle[n++] = kind >= 2 ? 8 : 0x10;
    if (kind) for (unsigned i = 0; i < 9; ++i) oracle[n++] = 0xff;
    else oracle[n++] = 0;
    old_send_object_t *o = object(sizeof(void *),&m);
    moq_result_t rc = write_object(&f,o);
    CHECK(rc == MOQ_OK);
    if (rc == MOQ_OK) expect_wire(&f,oracle,n);
    free(o); finish(&f);
}

static void unobserved_and_future_metadata(void)
{
    label = "explicit-d22-needs-observed-version-and-accepts-future-metadata"; ++rows;
    old_sender_cfg_t *cfg = config(4,1,1024);
    fixture_t f = {0}; bool started = start(&f,cfg,MOQ_VERSION_DRAFT_22,MOQ_MEDIA_PACKAGING_RAW);
    if (!started) { free(cfg); finish(&f); return; }
    moq_media_sender_t *unobserved = NULL;
    REQUIRE(moq_media_sender_test_construct((const moq_media_sender_cfg_t *)cfg,&unobserved) == MOQ_OK);
    free(cfg);
    moq_media_track_cfg_t tc; moq_media_track_cfg_init(&tc);
    tc.name = MOQ_BYTES_LITERAL("v"); tc.codec = MOQ_BYTES_LITERAL("av01");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO; tc.packaging = MOQ_MEDIA_PACKAGING_RAW; tc.bitrate = 1000;
    moq_media_track_t *track = NULL;
    REQUIRE(moq_media_sender_add_track(unobserved,&tc,&track) == MOQ_OK);
    struct { moq_media_metadata_t m; uint8_t future[16]; } meta;
    metadata(&meta.m,false,7,0); memset(meta.future,0xa5,sizeof(meta.future));
    meta.m.struct_size = sizeof(meta); meta.m.loc.struct_size = sizeof(meta.m.loc) + 16;
    old_send_object_t *o = object(sizeof(void *),&meta.m);
    const uint8_t data[] = {0xa1,0xb2,0xc3,0xd4};
    REQUIRE(moq_rcbuf_create(&f.alloc,data,sizeof(data),&o->payload) == MOQ_OK);
    moq_result_t rc = moq_media_sender_write(unobserved,track, (const moq_media_send_object_t *)o);
    CHECK(rc == MOQ_ERR_UNSUPPORTED);
    if (rc != MOQ_OK) moq_rcbuf_decref(o->payload);
    moq_media_sender_test_free(unobserved);
    rc = write_object(&f,o);
    CHECK(rc == MOQ_OK);
    const uint8_t oracle[] = {0x10,7};
    if (rc == MOQ_OK) expect_wire(&f,oracle,sizeof(oracle));
    for (size_t i = 0; i < sizeof(meta.future); ++i) CHECK(meta.future[i] == 0xa5);
    free(o); finish(&f);
}

static moq_result_t observed_write(fixture_t *f, old_send_object_t *o,
                                   moq_rcbuf_t **observer)
{
    const uint8_t data[] = {0xa1,0xb2,0xc3,0xd4};
    moq_result_t rc = moq_rcbuf_create(&f->alloc,data,sizeof(data),&o->payload);
    CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return rc;
    *observer = moq_rcbuf_incref(o->payload);
    rc = moq_media_sender_write(f->sender,f->track, (const moq_media_send_object_t *)o);
    if (rc != MOQ_OK) moq_rcbuf_decref(o->payload);
    return rc;
}

static void drop_properties(moq_media_send_backpressure_t policy)
{
    label = policy == MOQ_MEDIA_SEND_BP_DROP_GROUP ? "owned-properties-drop-group" :
                                                    "owned-properties-drop-to-keyframe";
    ++rows;
    old_sender_cfg_t *cfg = config(4,1,12);
    cfg->backpressure = policy;
    cfg->pre_ready_max_objects = cfg->queue_max_objects = 2;
    fixture_t f = {0}; bool started = start(&f,cfg,MOQ_VERSION_DRAFT_22,MOQ_MEDIA_PACKAGING_RAW);
    free(cfg);
    moq_rcbuf_t *a = NULL, *b = NULL, *c = NULL, *oversize = NULL;
    old_send_object_t *o = NULL;
    if (!started) goto cleanup;
    moq_media_metadata_t m; metadata(&m,false,7,0);
    o = object(sizeof(void *),&m); o->ends_group = false;
    moq_result_t rc = observed_write(&f,o,&a);
    CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) goto cleanup;
    free(o); o = object(sizeof(void *),&m);
    o->is_sync = o->starts_group = false; m.loc.timestamp = 8;
    rc = observed_write(&f,o,&b);
    CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) goto cleanup;
    CHECK(stats(&f).objects_written == 2 && stats(&f).objects_queued == 2 && stats(&f).bytes_queued == 12);
    CHECK(moq_rcbuf_refcount(a) == 2 && moq_rcbuf_refcount(b) == 2);

    /* An object that cannot fit must free its generated properties without
     * dropping the already-owned group, even with an incoming sync anchor. */
    free(o); o = object(sizeof(void *),&m);
    uint8_t config_bytes[16] = {0};
    m.loc.present |= MOQ_LOC_METADATA_HAS_VIDEO_CONFIG;
    m.loc.video_config = (moq_bytes_t){config_bytes,sizeof(config_bytes)};
    size_t freed = f.memory.frees;
    rc = observed_write(&f,o,&oversize);
    CHECK(rc == MOQ_ERR_WOULD_BLOCK);
    CHECK(f.memory.frees == freed + 1); /* only generated bytes; observer retains payload */
    CHECK(moq_rcbuf_refcount(a) == 2 && moq_rcbuf_refcount(b) == 2);
    CHECK(stats(&f).objects_written == 2 && stats(&f).objects_queued == 2 && stats(&f).bytes_queued == 12);
    CHECK(stats(&f).objects_dropped == 0 && stats(&f).groups_dropped == 0 && stats(&f).keyframes_dropped == 0);
    if (rc != MOQ_ERR_WOULD_BLOCK) goto cleanup;
    /* The observer keeps the refused payload alive; only generated bytes are
     * freed by the write. The caller payload decref above does not free it. */
    moq_rcbuf_decref(oversize); oversize = NULL;
    free(o); o = object(sizeof(void *),&m);
    metadata(&m,false,9,0);
    size_t live = f.memory.live;
    freed = f.memory.frees;
    rc = observed_write(&f,o,&c);
    CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) goto cleanup;
    CHECK(f.memory.live == live); /* two new buffers replace two evicted properties */
    CHECK(f.memory.frees == freed + 2);
    CHECK(moq_rcbuf_refcount(a) == 1 && moq_rcbuf_refcount(b) == 1 && moq_rcbuf_refcount(c) == 2);
    CHECK(stats(&f).objects_written == 3 && stats(&f).objects_queued == 1 && stats(&f).bytes_queued == 6);
    CHECK(stats(&f).objects_dropped == 2 && stats(&f).groups_dropped == 1 && stats(&f).keyframes_dropped == 1);
    CHECK(stats(&f).groups_abandoned == 0 && stats(&f).backpressure_stalls == 0);
    memset(&m,0xa5,sizeof(m)); free(o); o = NULL;
    const uint8_t oracle[] = {0x10,9};
    expect_wire(&f,oracle,sizeof(oracle));
    CHECK(moq_rcbuf_refcount(c) == 1);
    CHECK(stats(&f).objects_dropped == 2 && stats(&f).groups_dropped == 1 && stats(&f).keyframes_dropped == 1);
cleanup:
    moq_rcbuf_decref(a); moq_rcbuf_decref(b); moq_rcbuf_decref(c); moq_rcbuf_decref(oversize);
    free(o); finish(&f);
}

enum blocking_exit { EXIT_INTERRUPT, EXIT_CLOSED, EXIT_ORIGIN_CLOCK, EXIT_SLICE_CLOCK, EXIT_WAIT };
typedef struct blocking_script {
    enum blocking_exit kind;
    moq_endpoint_t *ep;
    unsigned clocks, waits;
} blocking_script_t;
static int blocking_clock(struct timespec *out, void *ctx)
{
    blocking_script_t *sc = ctx;
    ++sc->clocks;
    *out = (struct timespec){.tv_sec=sc->clocks > 2 ? 12 : 10,.tv_nsec=0};
    return (sc->kind == EXIT_ORIGIN_CLOCK && sc->clocks == 1) ||
           (sc->kind == EXIT_SLICE_CLOCK && sc->clocks == 2) ? -1 : 0;
}
static int blocking_wait(const struct timespec *abs, void *ctx)
{
    blocking_script_t *sc = ctx;
    ++sc->waits;
    CHECK(abs->tv_sec == 10 && abs->tv_nsec == 50000000);
    if (sc->kind == EXIT_INTERRUPT) moq_endpoint_set_interrupted(sc->ep,true);
    if (sc->kind == EXIT_CLOSED) CHECK(moq_endpoint_stop(sc->ep) == MOQ_OK);
    return sc->kind == EXIT_WAIT ? EINVAL : 0;
}
static void blocking_properties(enum blocking_exit kind)
{
    static const char *names[] = {"owned-properties-block-interrupted", "owned-properties-block-closed",
        "owned-properties-origin-clock-error", "owned-properties-slice-clock-error", "owned-properties-wait-error"};
    label = names[kind]; ++rows;
    old_sender_cfg_t *cfg = config(4,1,6);
    cfg->backpressure = MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT; cfg->block_timeout_us = 1000000;
    cfg->pre_ready_max_objects = cfg->queue_max_objects = 1;
    fixture_t f = {0}; bool started = start(&f,cfg,MOQ_VERSION_DRAFT_22,MOQ_MEDIA_PACKAGING_RAW);
    free(cfg);
    moq_endpoint_t *ep = NULL;
    moq_rcbuf_t *filler = NULL, *incoming = NULL;
    old_send_object_t *o = NULL;
    if (!started) goto cleanup;
    moq_media_metadata_t m; metadata(&m,false,7,0);
    o = object(sizeof(void *),&m);
    moq_result_t rc = observed_write(&f,o,&filler);
    CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) goto cleanup;
    ep = moq_endpoint_test_make_bare(); CHECK(ep != NULL);
    if (!ep) goto cleanup;
    moq_media_sender_test_set_ep(f.sender,ep);
    free(o); o = object(sizeof(void *),&m); m.loc.timestamp = 8;
    const uint8_t data[] = {0xa1,0xb2,0xc3,0xd4};
    rc = moq_rcbuf_create(&f.alloc,data,sizeof(data),&o->payload);
    CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) goto cleanup;
    incoming = moq_rcbuf_incref(o->payload);
    size_t live = f.memory.live, calls = f.memory.calls, frees = f.memory.frees;
    moq_media_sender_stats_t before = stats(&f);
    CHECK(before.objects_written == 1 && before.objects_queued == 1 && before.bytes_queued == 6);
    blocking_script_t script = {.kind=kind,.ep=ep};
    moq_media_sender_test_set_clock_hook(blocking_clock,&script);
    moq_media_sender_test_set_cond_wait_hook(blocking_wait,&script);
    rc = moq_media_sender_write(f.sender,f.track, (const moq_media_send_object_t *)o);
    moq_media_sender_test_set_clock_hook(NULL,NULL);
    moq_media_sender_test_set_cond_wait_hook(NULL,NULL);
    moq_result_t expected = kind == EXIT_INTERRUPT ? MOQ_ERR_INTERRUPTED :
                           kind == EXIT_CLOSED ? MOQ_ERR_CLOSED : MOQ_ERR_INTERNAL;
    CHECK(rc == expected);
    CHECK(script.clocks == (kind == EXIT_ORIGIN_CLOCK ? 1u : 2u));
    CHECK(script.waits == (kind == EXIT_ORIGIN_CLOCK || kind == EXIT_SLICE_CLOCK ? 0u : 1u));
    CHECK(f.memory.calls == calls + 1 && f.memory.frees == frees + 1 && f.memory.live == live);
    CHECK(moq_rcbuf_refcount(incoming) == 2 && moq_rcbuf_refcount(filler) == 2);
    moq_media_sender_stats_t after = stats(&f);
    CHECK(after.objects_written == before.objects_written && after.objects_queued == 1 && after.bytes_queued == 6);
    CHECK(after.objects_dropped == 0 && after.groups_dropped == 0 && after.keyframes_dropped == 0);
    CHECK(after.backpressure_stalls == before.backpressure_stalls + 1 && after.last_error == expected);
    if (rc != MOQ_OK) moq_rcbuf_decref(o->payload); /* actual result governs ownership */
    moq_media_sender_test_set_ep(f.sender,NULL);
    moq_endpoint_test_free_bare(ep); ep = NULL;
    free(o); o = NULL;
    if (rc != expected) goto cleanup;
    memset(&m,0xa5,sizeof(m));
    const uint8_t oracle[] = {0x10,7};
    expect_wire(&f,oracle,sizeof(oracle));
    CHECK(moq_rcbuf_refcount(filler) == 1);
cleanup:
    if (ep) { moq_media_sender_test_set_ep(f.sender,NULL); moq_endpoint_test_free_bare(ep); }
    moq_rcbuf_decref(filler); moq_rcbuf_decref(incoming);
    free(o); finish(&f);
}
/* MSF-01 6.1: a restarted publisher MUST start each track's Group IDs above
 * any it published before. The draft-22 companion starts every track of an
 * instance at the wallclock milliseconds of its first group (6.1's suggested
 * approach): the first media group on the wire and the first catalog group are
 * that base, and the next group follows it. The legacy companion keeps its
 * numbering from 0. */
static void group_ids(bool d22)
{
    label = d22 ? "d22-group-ids-start-at-instance-wallclock"
                : "legacy-group-ids-unchanged"; ++rows;
    const uint64_t base = 1700000000123ull;
    old_sender_cfg_t *cfg = config(4, d22 ? 1 : 0, 1024);
    g_clock_rc = 0; g_clock_ts = ms_clock(base);
    moq_media_sender_test_set_clock_hook(scripted_clock, NULL);
    fixture_t f = {0};
    bool started = start(&f, cfg, d22 ? MOQ_VERSION_DRAFT_22 : MOQ_VERSION_DRAFT_18,
                         MOQ_MEDIA_PACKAGING_RAW);
    moq_media_sender_test_set_clock_hook(NULL, NULL);
    free(cfg);
    if (!started) { finish(&f); return; }
    moq_media_metadata_t m; metadata(&m,false,7,0);
    for (unsigned g = 0; g < 2; ++g) {
        old_send_object_t *o = object(d22 ? sizeof(void *) : 0, &m);
        CHECK(write_object(&f, o) == MOQ_OK);
        free(o);
    }
    if (arm_peer(&f)) {
        CHECK(moq_media_sender_test_catalog_group(f.sender) == (d22 ? base : 0));
        uint64_t groups[4]; unsigned n = 0;
        moq_event_t e;
        while (moq_session_poll_events(f.peer, &e, 1)) {
            if (e.kind == MOQ_EVENT_OBJECT_RECEIVED && n < 4)
                groups[n++] = e.u.object_received.group_id;
            moq_event_cleanup(&e);
        }
        CHECK(n == 2);
        if (n == 2) {
            CHECK(groups[0] == (d22 ? base : 0));
            CHECK(groups[1] == groups[0] + 1);
        }
    }
    finish(&f);
}

/* The Group ID base is resolved before the sender exists: a clock that
 * cannot be read, or reads before the epoch, is MOQ_ERR_INTERNAL; a reading
 * past MOQ_QUIC_VARINT_MAX is MOQ_ERR_LIMIT. Nothing is left allocated, and
 * the legacy companion, which numbers from 0, never reads the clock. AUTO,
 * explicit or absent, reads it and fails the same way. */
static void group_base_refusal(unsigned kind)
{
    static const char *const names[] = {
        "group-base-clock-failure", "group-base-before-epoch",
        "group-base-past-varint-max", "group-base-seconds-overflow",
        "group-base-legacy-ignores-clock", "group-base-absent-auto-clock-failure",
        "group-base-explicit-auto-past-varint-max",
    };
    label = names[kind]; ++rows;
    g_clock_rc = 0;
    moq_result_t want = MOQ_ERR_INTERNAL;
    switch (kind) {
    case 0: g_clock_rc = -1; break;
    case 1: g_clock_ts = (struct timespec){.tv_sec=-1,.tv_nsec=0}; break;
    case 2: g_clock_ts = ms_clock(MOQ_QUIC_VARINT_MAX + 1u);
            want = MOQ_ERR_LIMIT; break;
    case 3: g_clock_ts = (struct timespec){
                .tv_sec=(time_t)(MOQ_QUIC_VARINT_MAX / 1000u + 1u),.tv_nsec=0};
            want = MOQ_ERR_LIMIT; break;
    case 4: g_clock_rc = -1; want = MOQ_OK; break;
    case 5: g_clock_rc = -1; break;
    default: g_clock_ts = ms_clock(MOQ_QUIC_VARINT_MAX + 1u);
             want = MOQ_ERR_LIMIT; break;
    }
    old_sender_cfg_t *cfg = kind == 5 ? config(0, 0, 1024) :
        config(4, kind == 4 ? 0 : kind == 6 ? MOQ_MEDIA_COMPANION_AUTO : 1, 1024);
    allocation_t memory = {0};
    moq_alloc_t alloc = {.ctx=&memory,.alloc=allocate,.realloc=resize,.free=release};
    moq_media_sender_test_set_alloc(&alloc);
    moq_media_sender_test_set_clock_hook(scripted_clock, NULL);
    moq_media_sender_t *s = (moq_media_sender_t *)(uintptr_t)1;
    CHECK(moq_media_sender_test_construct((const moq_media_sender_cfg_t *)cfg, &s) == want);
    moq_media_sender_test_set_clock_hook(NULL, NULL);
    if (want == MOQ_OK) CHECK(s != NULL);
    else CHECK(s == NULL);
    if (s) moq_media_sender_test_free(s);
    moq_media_sender_test_set_alloc(NULL);
    CHECK(memory.live == 0);
    g_clock_rc = 0;
    free(cfg);
}

/* The last Group ID is usable, and nothing goes past it. With the base at
 * MOQ_QUIC_VARINT_MAX the initial catalog and the first media group take
 * that ID; a later object of the open group is still accepted; a new media
 * group, and every catalog change that would need a new catalog group (add,
 * remove, complete), is refused with MOQ_ERR_LIMIT before it takes ownership
 * or changes state, and the sender stays healthy. */
static void group_id_ceiling(void)
{
    label = "group-id-final-valid-and-exhaustion"; ++rows;
    old_sender_cfg_t *cfg = config(4, 1, 1024);
    g_clock_rc = 0; g_clock_ts = ms_clock(MOQ_QUIC_VARINT_MAX);
    moq_media_sender_test_set_clock_hook(scripted_clock, NULL);
    fixture_t f = {0};
    bool started = start(&f, cfg, MOQ_VERSION_DRAFT_22, MOQ_MEDIA_PACKAGING_RAW);
    moq_media_sender_test_set_clock_hook(NULL, NULL);
    free(cfg);
    if (!started) { finish(&f); return; }
    moq_media_metadata_t m; metadata(&m,false,7,0);
    old_send_object_t *o = object(sizeof(void *), &m);
    o->ends_group = false;
    CHECK(write_object(&f, o) == MOQ_OK);             /* opens group MAX */
    o->is_sync = false; o->starts_group = false;
    CHECK(write_object(&f, o) == MOQ_OK);             /* continues group MAX */
    o->is_sync = true; o->starts_group = true;
    CHECK(write_object(&f, o) == MOQ_ERR_LIMIT);      /* no group after MAX */
    CHECK(stats(&f).objects_written == 2);
    free(o);
    if (arm_peer(&f)) {
        CHECK(moq_media_sender_is_ready(f.sender));
        CHECK(moq_media_sender_test_catalog_group(f.sender) == MOQ_QUIC_VARINT_MAX);
        unsigned n = 0;
        moq_event_t e;
        while (moq_session_poll_events(f.peer, &e, 1)) {
            if (e.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                ++n;
                CHECK(e.u.object_received.group_id == MOQ_QUIC_VARINT_MAX);
            }
            moq_event_cleanup(&e);
        }
        CHECK(n == 2);
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = MOQ_BYTES_LITERAL("late"); tc.codec = MOQ_BYTES_LITERAL("av01");
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO; tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.bitrate = 1000;
        moq_media_track_t *late = NULL;
        CHECK(moq_media_sender_add_track(f.sender, &tc, &late) == MOQ_ERR_LIMIT);
        CHECK(late == NULL);
        CHECK(moq_media_sender_remove_track(f.sender, f.track) == MOQ_ERR_LIMIT);
        CHECK(moq_media_sender_complete(f.sender) == MOQ_ERR_LIMIT);
        moq_media_sender_test_pump(f.sender, f.local, 1005);
        CHECK(!moq_media_sender_is_fatal(f.sender));
        CHECK(moq_media_sender_test_catalog_group(f.sender) == MOQ_QUIC_VARINT_MAX);
    }
    finish(&f);
}

/* One catalog group left: the change that needs it is accepted, a second
 * change made before it is staged coalesces into the same generation, and
 * once that generation holds MOQ_QUIC_VARINT_MAX the next change is refused. */
static void catalog_last_group(void)
{
    label = "catalog-final-generation"; ++rows;
    old_sender_cfg_t *cfg = config(4, 1, 1024);
    g_clock_rc = 0; g_clock_ts = ms_clock(MOQ_QUIC_VARINT_MAX - 1u);
    moq_media_sender_test_set_clock_hook(scripted_clock, NULL);
    fixture_t f = {0};
    bool started = start(&f, cfg, MOQ_VERSION_DRAFT_22, MOQ_MEDIA_PACKAGING_RAW);
    moq_media_sender_test_set_clock_hook(NULL, NULL);
    free(cfg);
    if (!started) { finish(&f); return; }
    if (arm_peer(&f)) {
        drain_events(f.peer);
        CHECK(moq_media_sender_test_catalog_group(f.sender) ==
              MOQ_QUIC_VARINT_MAX - 1u);
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.codec = MOQ_BYTES_LITERAL("av01");
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO; tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.bitrate = 1000;
        moq_media_track_t *a = NULL, *b = NULL, *c = NULL;
        tc.name = MOQ_BYTES_LITERAL("a");
        CHECK(moq_media_sender_add_track(f.sender, &tc, &a) == MOQ_OK);
        tc.name = MOQ_BYTES_LITERAL("b");
        CHECK(moq_media_sender_add_track(f.sender, &tc, &b) == MOQ_OK);
        moq_media_sender_test_pump(f.sender, f.local, 1005);
        CHECK(moq_simpair_run_until_quiescent(f.pair, 16, NULL) == MOQ_OK);
        moq_media_sender_test_pump(f.sender, f.local, 1006);
        CHECK(moq_media_sender_test_catalog_group(f.sender) == MOQ_QUIC_VARINT_MAX);
        tc.name = MOQ_BYTES_LITERAL("c");
        CHECK(moq_media_sender_add_track(f.sender, &tc, &c) == MOQ_ERR_LIMIT);
        CHECK(c == NULL);
        CHECK(!moq_media_sender_is_fatal(f.sender));
    }
    finish(&f);
}

/* An AUTO sender: construct with a scripted Group ID base, register one video
 * track before any session exists, then observe a `version` session. */
static bool start_auto(fixture_t *f, old_sender_cfg_t *cfg, uint64_t base,
                       moq_media_packaging_t packaging, moq_bytes_t init)
{
    f->alloc = (moq_alloc_t){.ctx=&f->memory,.alloc=allocate,.realloc=resize,.free=release};
    moq_media_sender_test_set_alloc(&f->alloc);
    g_clock_rc = 0; g_clock_ts = ms_clock(base);
    moq_media_sender_test_set_clock_hook(scripted_clock, NULL);
    moq_result_t rc = moq_media_sender_test_construct((const moq_media_sender_cfg_t *)cfg, &f->sender);
    moq_media_sender_test_set_clock_hook(NULL, NULL);
    CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return false;
    CHECK(moq_media_sender_test_companion(f->sender) == MOQ_MEDIA_COMPANION_AUTO);
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = MOQ_BYTES_LITERAL("v"); tc.codec = MOQ_BYTES_LITERAL("av01");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO; tc.packaging = packaging; tc.bitrate = 1000;
    tc.init_data = init;
    rc = moq_media_sender_add_track(f->sender, &tc, &f->track);
    CHECK(rc == MOQ_OK);
    return rc == MOQ_OK;
}
static bool observe_session(fixture_t *f, moq_version_t version)
{
    moq_simpair_cfg_t pc = MOQ_SIMPAIR_CFG_INIT;
    pc.alloc = moq_alloc_default(); pc.version = version;
    pc.client_send_request_capacity = pc.server_send_request_capacity = true;
    pc.client_initial_request_capacity = pc.server_initial_request_capacity = 32;
    bool ok = moq_simpair_create(&pc, &f->pair) == MOQ_OK &&
              moq_simpair_start(f->pair) == MOQ_OK &&
              moq_simpair_run_until_quiescent(f->pair, 16, NULL) == MOQ_OK;
    CHECK(ok);
    if (!ok) return false;
    f->local = moq_simpair_client(f->pair); f->peer = moq_simpair_server(f->pair);
    drain_events(f->local); drain_events(f->peer);
    moq_media_sender_test_pump(f->sender, f->local, 1000);
    return true;
}

/* The default companion of a caller whose config and object predate it
 * follows the negotiated draft: the catalog form, the Group ID base, and the
 * LOC block all match the companion the draft selects, for an object queued
 * before the draft was known and for the next group written after. Without a
 * metadata tail the draft-22 block is generated from the typed fields: a
 * capture time is an unscaled Unix-microsecond Timestamp (LOC-04 2.3.1.1); a
 * presentation time alone is media time, sent with Timescale 1000000
 * (2.3.1.2). With a tail, the tail is authoritative on draft 22. Drafts 16/18
 * keep LOC-01 and ignore the tail. */
static void auto_resolution(moq_version_t version, bool before_latch, bool with_metadata,
                            bool pts_only, uint64_t pts)
{
    label = version == MOQ_VERSION_DRAFT_22 ? "auto-resolves-draft22-companion"
                                            : "auto-resolves-legacy-companion"; ++rows;
    const bool d22 = version == MOQ_VERSION_DRAFT_22;
    const uint64_t base = 1700000000123ull;
    static const uint8_t init[] = {0x01,0x02,0x03};
    old_sender_cfg_t *cfg = config(0, 0, 1024);   /* field absent */
    fixture_t f = {0};
    old_send_object_t *o = NULL;
    bool started = start_auto(&f, cfg, base, MOQ_MEDIA_PACKAGING_RAW,
                              (moq_bytes_t){init, sizeof(init)});
    free(cfg);
    if (!started) goto done;
    moq_media_metadata_t m; metadata(&m,false,7,0);
    o = with_metadata ? object(sizeof(void *), &m) : object(0, NULL);
    if (pts_only) { o->has_capture_time = false; o->capture_time_us = 0; }
    o->presentation_time_us = pts;
    if (before_latch) {
        CHECK(write_object(&f, o) == MOQ_OK);
        CHECK(moq_media_sender_test_companion(f.sender) == MOQ_MEDIA_COMPANION_AUTO);
    }
    if (!observe_session(&f, version)) goto done;
    CHECK(moq_media_sender_test_companion(f.sender) ==
          (d22 ? MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC
               : MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01));
    CHECK(stats(&f).last_error == MOQ_OK && !moq_media_sender_is_fatal(f.sender));
    CHECK(write_object(&f, o) == MOQ_OK);
    const unsigned want = before_latch ? 2u : 1u;

    moq_rcbuf_t *json = NULL;
    CHECK(moq_media_sender_test_build_catalog(f.sender, &json) == MOQ_OK && json);
    if (json) {
        char text[1024] = {0};
        size_t n = moq_rcbuf_len(json) < sizeof(text) - 1 ? moq_rcbuf_len(json) : sizeof(text) - 1;
        memcpy(text, moq_rcbuf_data(json), n);
        CHECK((strstr(text, "\"version\":\"draft-01\"") != NULL) == d22);
        CHECK((strstr(text, "\"initDataList\"") != NULL) == d22);
        CHECK((strstr(text, "\"initRef\"") != NULL) == d22);
        CHECK((strstr(text, "\"initData\"") != NULL) == !d22);
        moq_rcbuf_decref(json);
    }

    /* Timestamp 99 (capture) or a presentation time below 64: one integer
     * byte on the vi64 drafts; the QUIC varint of draft 16 needs two bytes
     * from 64. */
    const uint8_t t = (uint8_t)(pts_only ? pts : 99);
    const uint8_t loc04_metadata[] = {0x10,7};
    /* Marking (9), then Timestamp (16): deltas 9 and 7. */
    const uint8_t loc04_capture[] = {0x09,0x01,0x20,0x07,t};
    /* Timescale (8) = 1000000, Marking (9), Timestamp (16): deltas 8, 1, 7. */
    const uint8_t loc04_media[] = {0x08,0xcf,0x42,0x40,0x01,0x01,0x20,0x07,t};
    const uint8_t loc16[] = {2,0x40,t,2,0x20};
    const uint8_t loc18[] = {2,t,2,0x20};    /* also draft 16 below 64 */
    const bool wide16 = version == MOQ_VERSION_DRAFT_16 && t >= 64;
    const uint8_t *oracle = d22 ? (with_metadata ? loc04_metadata :
                                   pts_only ? loc04_media : loc04_capture) :
                            wide16 ? loc16 : loc18;
    size_t n = d22 ? (with_metadata ? sizeof(loc04_metadata) :
                      pts_only ? sizeof(loc04_media) : sizeof(loc04_capture)) :
               wide16 ? sizeof(loc16) : sizeof(loc18);
    if (arm_peer(&f)) {
        unsigned seen = 0;
        moq_event_t e;
        while (moq_session_poll_events(f.peer, &e, 1)) {
            if (e.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                const moq_rcbuf_t *props = e.u.object_received.properties;
                CHECK(e.u.object_received.group_id == (d22 ? base : 0) + seen);
                CHECK(moq_rcbuf_len(props) == n &&
                      memcmp(moq_rcbuf_data(props), oracle, n) == 0);
                if (d22) {
                    /* What the peer's parser reads from those properties:
                     * the caller's tail unchanged, else a capture time as
                     * Unix microseconds or a presentation time as media time
                     * in microseconds. */
                    moq_loc_metadata_t got;
                    CHECK(moq_loc_parse_metadata(MOQ_VERSION_DRAFT_22, MOQ_LOC_PROFILE_04,
                              (moq_bytes_t){moq_rcbuf_data(props), moq_rcbuf_len(props)},
                              &got, sizeof(got)) == MOQ_OK);
                    uint32_t want = MOQ_LOC_METADATA_HAS_TIMESTAMP;
                    if (!with_metadata) want |= MOQ_LOC_METADATA_HAS_MARKING;
                    if (!with_metadata && pts_only) want |= MOQ_LOC_METADATA_HAS_TIMESCALE;
                    CHECK(got.present == want);
                    CHECK(got.timestamp == (with_metadata ? 7u : t));
                    CHECK(got.timescale == (!with_metadata && pts_only ? 1000000u : 0u));
                }
                ++seen;
            }
            moq_event_cleanup(&e);
        }
        CHECK(seen == want);
        CHECK(stats(&f).objects_sent == want && !moq_media_sender_is_fatal(f.sender));
        CHECK(moq_media_sender_test_catalog_group(f.sender) == (d22 ? base : 0));
    }
done:
    free(o);
    finish(&f);
}

/* CMSF-01 3.1 under AUTO: a CMAF track without init_data is accepted while the
 * draft is unknown. Draft 22 then blocks the companion -- UNSUPPORTED, nothing
 * published, no fatal -- until the track is removed; drafts 16/18 publish it.
 * Once resolved to draft 22, add_track refuses such a track outright. */
static void auto_cmaf_without_init(moq_version_t version)
{
    label = version == MOQ_VERSION_DRAFT_22 ? "auto-draft22-cmaf-without-init-blocks"
                                            : "auto-legacy-cmaf-without-init-publishes"; ++rows;
    const bool d22 = version == MOQ_VERSION_DRAFT_22;
    old_sender_cfg_t *cfg = config(4, MOQ_MEDIA_COMPANION_AUTO, 1024);
    fixture_t f = {0};
    bool started = start_auto(&f, cfg, 1700000000123ull, MOQ_MEDIA_PACKAGING_CMAF,
                              (moq_bytes_t){NULL, 0});
    free(cfg);
    if (!started || !observe_session(&f, version)) { finish(&f); return; }
    CHECK(moq_media_sender_test_companion(f.sender) ==
          (d22 ? MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC
               : MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01));
    CHECK(!moq_media_sender_is_fatal(f.sender));
    CHECK(moq_session_state(f.local) == MOQ_SESS_ESTABLISHED);
    moq_action_t a;
    size_t actions = moq_session_poll_actions(f.local, &a, 1);
    CHECK((actions != 0) == !d22);
    if (actions) moq_action_cleanup(&a);
    if (d22) {
        CHECK(stats(&f).last_error == MOQ_ERR_UNSUPPORTED);
        old_send_object_t *o = object(0, NULL);
        CHECK(write_object(&f, o) == MOQ_ERR_UNSUPPORTED);
        free(o);
        CHECK(stats(&f).objects_written == 0);
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = MOQ_BYTES_LITERAL("w"); tc.codec = MOQ_BYTES_LITERAL("av01");
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO; tc.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        tc.bitrate = 1000;
        moq_media_track_t *late = NULL;
        CHECK(moq_media_sender_add_track(f.sender, &tc, &late) == MOQ_ERR_INVAL && !late);
        CHECK(moq_media_sender_remove_track(f.sender, f.track) == MOQ_OK);
        moq_media_sender_test_pump(f.sender, f.local, 1001);
        actions = moq_session_poll_actions(f.local, &a, 1);
        CHECK(actions != 0);
        if (actions) moq_action_cleanup(&a);
    } else {
        CHECK(stats(&f).last_error == MOQ_OK);
    }
    CHECK(!moq_media_sender_is_fatal(f.sender));
    finish(&f);
}

/* -- Typed capture time against a Track Properties Timescale ---------------
 * LOC-04 2.3.1.1: a Timestamp without Timescale is Unix microseconds; 2.3.1.2:
 * a Timescale (Track or Object scope) makes it media time. A capture time sent
 * unscaled on a track whose properties declare a Timescale would be read at
 * that scale, so an AUTO sender refuses it whenever the object may go out as
 * draft 22. */

typedef struct scaled {
    fixture_t f;
    moq_endpoint_t *ep;
    uint8_t props[16];   /* the caller's buffer, poisoned after add_track */
} scaled_t;

/* Construct with `companion` (4-byte tail), optionally attach a bare endpoint
 * offering `offer`, register one RAW track carrying `props`, then observe a
 * `version` session (0: none). */
static bool scaled_start(scaled_t *x, uint32_t companion, const moq_version_t *offer,
                         size_t offer_n, const uint8_t *props, size_t props_n,
                         moq_media_type_t type, moq_version_t version, uint32_t budget,
                         uint32_t max_objects, moq_media_send_backpressure_t bp)
{
    fixture_t *f = &x->f;
    f->alloc = (moq_alloc_t){.ctx=&f->memory,.alloc=allocate,.realloc=resize,.free=release};
    moq_media_sender_test_set_alloc(&f->alloc);
    old_sender_cfg_t *cfg = config(4, companion, budget);
    if (max_objects) cfg->pre_ready_max_objects = cfg->queue_max_objects = max_objects;
    if (bp) cfg->backpressure = bp;
    g_clock_rc = 0; g_clock_ts = ms_clock(1700000000123ull);
    moq_media_sender_test_set_clock_hook(scripted_clock, NULL);
    moq_result_t rc = moq_media_sender_test_construct((const moq_media_sender_cfg_t *)cfg, &f->sender);
    moq_media_sender_test_set_clock_hook(NULL, NULL);
    free(cfg);
    CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return false;
    if (offer_n) {
        x->ep = moq_endpoint_test_make_bare();
        CHECK(x->ep != NULL);
        if (!x->ep) return false;
        moq_endpoint_test_set_offer(x->ep, offer, offer_n);
        moq_media_sender_test_set_ep(f->sender, x->ep);
    }
    if (props_n) memcpy(x->props, props, props_n);
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init_sized(&tc, sizeof(tc));
    tc.name = MOQ_BYTES_LITERAL("v"); tc.media_type = type; tc.bitrate = 1000;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = type == MOQ_MEDIA_TYPE_AUDIO ? MOQ_BYTES_LITERAL("opus")
                                            : MOQ_BYTES_LITERAL("av01");
    if (type == MOQ_MEDIA_TYPE_AUDIO) {
        tc.samplerate = 48000; tc.channel_config = MOQ_BYTES_LITERAL("2");
    }
    tc.track_properties = (moq_bytes_t){x->props, props_n};
    rc = moq_media_sender_add_track(f->sender, &tc, &f->track);
    CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return false;
    memset(x->props, 0xa5, sizeof(x->props));   /* the copy is what counts */
    if (!version) return true;
    moq_simpair_cfg_t pc = MOQ_SIMPAIR_CFG_INIT;
    pc.alloc = moq_alloc_default(); pc.version = version;
    pc.client_send_request_capacity = pc.server_send_request_capacity = true;
    pc.client_initial_request_capacity = pc.server_initial_request_capacity = 32;
    bool ok = moq_simpair_create(&pc, &f->pair) == MOQ_OK &&
              moq_simpair_start(f->pair) == MOQ_OK &&
              moq_simpair_run_until_quiescent(f->pair, 16, NULL) == MOQ_OK;
    CHECK(ok);
    if (!ok) return false;
    f->local = moq_simpair_client(f->pair); f->peer = moq_simpair_server(f->pair);
    drain_events(f->local); drain_events(f->peer);
    moq_media_sender_test_pump(f->sender, f->local, 1000);
    return true;
}
static void scaled_finish(scaled_t *x)
{
    if (x->f.sender && x->ep) moq_media_sender_test_set_ep(x->f.sender, NULL);
    if (x->f.sender) moq_media_sender_test_free(x->f.sender);
    x->f.sender = NULL;
    moq_media_sender_test_set_alloc(NULL);
    if (x->f.pair) moq_simpair_destroy(x->f.pair);
    if (x->ep) moq_endpoint_test_free_bare(x->ep);
    CHECK(x->f.memory.live == 0);
}
/* A typed write: capture time 48000 (or none), no metadata tail. */
static moq_result_t typed_write(fixture_t *f, bool capture)
{
    old_send_object_t *o = object(0, NULL);
    o->has_capture_time = capture;
    o->capture_time_us = capture ? 48000u : 0u;
    o->presentation_time_us = 17;
    moq_result_t rc = write_object(f, o);
    free(o);
    return rc;
}

static const uint8_t k_scale48k[] = {0x08,0xc0,0xbb,0x80};

/* Before any session: refused exactly when the endpoint offers draft 22.
 * After a mixed offer resolves, the negotiated draft decides. Explicit
 * companions are unaffected. Track Properties 08 01 are valid on every
 * draft. */
static void scale_offer_matrix(void)
{
    label = "track-timescale-capture-offer-matrix"; ++rows;
    static const uint8_t props[] = {0x08,0x01};
    static const moq_version_t o16[] = {MOQ_VERSION_DRAFT_16},
        o18[] = {MOQ_VERSION_DRAFT_18},
        o1618[] = {MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_16},
        o22[] = {MOQ_VERSION_DRAFT_22},
        omix[] = {MOQ_VERSION_DRAFT_22, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_16};
    static const struct {
        const moq_version_t *offer; size_t n; uint32_t companion;
        moq_version_t version; moq_result_t want;
    } r[] = {
        {o16, 1, MOQ_MEDIA_COMPANION_AUTO, 0, MOQ_OK},
        {o18, 1, MOQ_MEDIA_COMPANION_AUTO, 0, MOQ_OK},
        {o1618, 2, MOQ_MEDIA_COMPANION_AUTO, 0, MOQ_OK},
        {o22, 1, MOQ_MEDIA_COMPANION_AUTO, 0, MOQ_ERR_INVAL},
        {omix, 3, MOQ_MEDIA_COMPANION_AUTO, 0, MOQ_ERR_INVAL},
        {omix, 3, MOQ_MEDIA_COMPANION_AUTO, MOQ_VERSION_DRAFT_18, MOQ_OK},
        {omix, 3, MOQ_MEDIA_COMPANION_AUTO, MOQ_VERSION_DRAFT_16, MOQ_OK},
        {omix, 3, MOQ_MEDIA_COMPANION_AUTO, MOQ_VERSION_DRAFT_22, MOQ_ERR_INVAL},
        {omix, 3, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01, 0, MOQ_OK},
        {omix, 3, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01, MOQ_VERSION_DRAFT_18, MOQ_OK},
        /* Explicit D22 keeps its own rule: no write before draft 22. */
        {omix, 3, MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC, 0, MOQ_ERR_UNSUPPORTED},
    };
    for (size_t i = 0; i < sizeof(r) / sizeof(*r); ++i) {
        scaled_t x; memset(&x, 0, sizeof(x));
        if (scaled_start(&x, r[i].companion, r[i].offer, r[i].n, props, sizeof(props),
                         MOQ_MEDIA_TYPE_VIDEO, r[i].version, 1024, 0, 0)) {
            moq_result_t rc = typed_write(&x.f, true);
            if (rc != r[i].want) fprintf(stderr, "offer row %zu: rc=%d\n", i, (int)rc);
            CHECK(rc == r[i].want);
            if (rc == MOQ_ERR_INVAL)
                CHECK(stats(&x.f).last_error == MOQ_ERR_INVAL &&
                      stats(&x.f).objects_written == 0);
            /* The presentation-time form is never refused for the scale. */
            if (r[i].want == MOQ_ERR_INVAL)
                CHECK(typed_write(&x.f, false) == MOQ_OK);
        }
        scaled_finish(&x);
    }
}

/* Resolved draft 22: which Track Properties count as declaring a Timescale.
 * A present zero scale counts; an immutable inner list counts; well-formed
 * unknown and repeated unknown fields are skipped; a block the LOC-04 parser
 * refuses (duplicate known field, invalid known value) may hide a Timescale
 * and is refused the same way. */
static void scale_detector(void)
{
    label = "track-timescale-detector"; ++rows;
    static const uint8_t direct[] = {0x08,0xc0,0xbb,0x80};
    static const uint8_t wrapped[] = {0x0b,0x04,0x08,0xc0,0xbb,0x80};
    static const uint8_t zero[] = {0x08,0x00};
    static const uint8_t around[] = {0x08,0xc0,0xbb,0x80,0x36,0x05,0x00,0x06};
    static const uint8_t unknown_only[] = {0x3e,0x05,0x00,0x06};
    static const uint8_t dup_known[] = {0x0d,0x01,0xaa,0x00,0x01,0xbb};
    static const uint8_t bad_marking[] = {0x09,0x00};
    static const struct { const uint8_t *p; size_t n; moq_result_t want; } r[] = {
        {direct, sizeof(direct), MOQ_ERR_INVAL},
        {wrapped, sizeof(wrapped), MOQ_ERR_INVAL},
        {zero, sizeof(zero), MOQ_ERR_INVAL},
        {around, sizeof(around), MOQ_ERR_INVAL},
        {unknown_only, sizeof(unknown_only), MOQ_OK},
        {dup_known, sizeof(dup_known), MOQ_ERR_INVAL},
        {bad_marking, sizeof(bad_marking), MOQ_ERR_INVAL},
        {NULL, 0, MOQ_OK},
    };
    for (size_t i = 0; i < sizeof(r) / sizeof(*r); ++i) {
        for (unsigned type = 0; type < 2; ++type) {
            scaled_t x; memset(&x, 0, sizeof(x));
            if (scaled_start(&x, MOQ_MEDIA_COMPANION_AUTO, NULL, 0, r[i].p, r[i].n,
                             type ? MOQ_MEDIA_TYPE_AUDIO : MOQ_MEDIA_TYPE_VIDEO,
                             MOQ_VERSION_DRAFT_22, 1024, 0, 0)) {
                moq_result_t rc = typed_write(&x.f, true);
                if (rc != r[i].want) fprintf(stderr, "detector row %zu/%u: rc=%d\n", i, type, (int)rc);
                CHECK(rc == r[i].want);
                /* The refusal is specific to the typed capture time: the
                 * same track still takes the presentation-time form. */
                rc = typed_write(&x.f, false);
                if (rc != MOQ_OK) fprintf(stderr, "detector row %zu/%u: pts rc=%d\n", i, type, (int)rc);
                CHECK(rc == MOQ_OK);
            }
            scaled_finish(&x);
        }
    }
}

/* Resolved draft 22, Timescale 48000 track: only a typed capture time with no
 * metadata tail is refused. A tail is present only when the whole pointer is
 * covered and non-NULL; an explicit tail goes out unchanged (its Timestamp
 * without Timescale is media time at the track's scale -- that is the
 * caller's declaration); an invalid explicit tail fails its own validation;
 * the presentation-time form carries its own Timescale. */
static void scale_metadata_boundaries(void)
{
    label = "track-timescale-metadata-boundaries"; ++rows;
    moq_media_metadata_t valid; metadata(&valid, false, 7, 0);
    moq_media_metadata_t invalid; metadata(&invalid, false, 7, 0);
    invalid.loc.present = 0;
    static const struct { size_t tail; int meta; moq_result_t want; } r[] = {
        {0, 0, MOQ_ERR_INVAL},                    /* V0 object */
        {sizeof(void *) - 1, 1, MOQ_ERR_INVAL},   /* pointer not covered */
        {sizeof(void *), 0, MOQ_ERR_INVAL},       /* covered NULL */
        {sizeof(void *), 2, MOQ_ERR_INVAL},       /* covered, invalid record */
        {sizeof(void *), 1, MOQ_OK},              /* covered, valid record */
    };
    for (size_t i = 0; i < sizeof(r) / sizeof(*r); ++i) {
        scaled_t x; memset(&x, 0, sizeof(x));
        if (scaled_start(&x, MOQ_MEDIA_COMPANION_AUTO, NULL, 0, k_scale48k,
                         sizeof(k_scale48k), MOQ_MEDIA_TYPE_VIDEO,
                         MOQ_VERSION_DRAFT_22, 1024, 0, 0)) {
            old_send_object_t *o = object(r[i].tail,
                r[i].meta == 1 ? &valid : r[i].meta == 2 ? &invalid : NULL);
            o->capture_time_us = 48000;
            moq_result_t rc = write_object(&x.f, o);
            free(o);
            if (rc != r[i].want) fprintf(stderr, "metadata row %zu: rc=%d\n", i, (int)rc);
            CHECK(rc == r[i].want);
            if (rc == MOQ_OK) {
                const uint8_t tail_bytes[] = {0x10,7};
                expect_wire(&x.f, tail_bytes, sizeof(tail_bytes));
            }
        }
        scaled_finish(&x);
    }
    /* The presentation-time form: object Timescale 1000000, marking, PTS. */
    scaled_t x; memset(&x, 0, sizeof(x));
    if (scaled_start(&x, MOQ_MEDIA_COMPANION_AUTO, NULL, 0, k_scale48k,
                     sizeof(k_scale48k), MOQ_MEDIA_TYPE_VIDEO,
                     MOQ_VERSION_DRAFT_22, 1024, 0, 0)) {
        CHECK(typed_write(&x.f, false) == MOQ_OK);
        const uint8_t media[] = {0x08,0xcf,0x42,0x40,0x01,0x01,0x20,0x07,17};
        expect_wire(&x.f, media, sizeof(media));
    }
    scaled_finish(&x);
}

/* The refusal happens before admission, so nothing is evicted, dropped or
 * reset for it. The queue is genuinely full (both object slots taken) and
 * the oldest queued object continues a group whose subgroup is open on the
 * wire, so admitting the refused sync object under DROP_TO_KEYFRAME would have
 * to evict that group and reset its subgroup. After the refusal the queued
 * objects drain intact, in their groups, and a valid write is accepted. */
static moq_result_t meta_write(fixture_t *f, moq_media_metadata_t *m,
                               bool sync, bool ends, moq_rcbuf_t **observer)
{
    old_send_object_t *o = object(sizeof(void *), m);
    o->is_sync = o->starts_group = sync;
    o->ends_group = ends;
    moq_result_t rc = observed_write(f, o, observer);
    free(o);
    return rc;
}
static void scale_refusal_owns_nothing(void)
{
    label = "track-timescale-refusal-before-admission"; ++rows;
    scaled_t x; memset(&x, 0, sizeof(x));
    moq_rcbuf_t *a = NULL, *b = NULL, *c = NULL, *incoming = NULL;
    if (!scaled_start(&x, MOQ_MEDIA_COMPANION_AUTO, NULL, 0, k_scale48k,
                      sizeof(k_scale48k), MOQ_MEDIA_TYPE_VIDEO, MOQ_VERSION_DRAFT_22,
                      1024, 2, MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME)) goto done;
    fixture_t *f = &x.f;
    moq_media_metadata_t m; metadata(&m, false, 7, 0);
    /* A opens group 0 and is sent; the subgroup stays open on the wire. */
    CHECK(meta_write(f, &m, true, false, &a) == MOQ_OK);
    if (!arm_peer(f)) goto done;
    unsigned got = 0;
    moq_event_t e;
    while (moq_session_poll_events(f->peer, &e, 1)) {
        if (e.kind == MOQ_EVENT_OBJECT_RECEIVED) ++got;
        moq_event_cleanup(&e);
    }
    CHECK(got == 1 && stats(f).objects_sent == 1 && stats(f).objects_queued == 0);
    /* B continues the open group 0; C opens group 1. Both slots are taken. */
    CHECK(meta_write(f, &m, false, true, &b) == MOQ_OK);
    CHECK(meta_write(f, &m, true, true, &c) == MOQ_OK);
    moq_media_sender_stats_t before = stats(f);
    CHECK(before.objects_queued == 2);
    CHECK(b && c && moq_rcbuf_refcount(b) == 2 && moq_rcbuf_refcount(c) == 2);
    /* The refused typed capture time: a sync object opening a new group. */
    old_send_object_t *o = object(0, NULL);
    o->capture_time_us = 48000;
    moq_result_t rc = observed_write(f, o, &incoming);
    free(o);
    CHECK(rc == MOQ_ERR_INVAL);
    moq_media_sender_stats_t after = stats(f);
    CHECK(after.last_error == MOQ_ERR_INVAL);
    CHECK(after.objects_written == before.objects_written &&
          after.objects_queued == 2 && after.bytes_queued == before.bytes_queued &&
          after.objects_dropped == 0 && after.groups_dropped == 0 &&
          after.keyframes_dropped == 0 && after.backpressure_stalls == 0);
    CHECK(moq_rcbuf_refcount(b) == 2 && moq_rcbuf_refcount(c) == 2);
    CHECK(incoming && moq_rcbuf_refcount(incoming) == 1);
    /* Drain: B completes group 0 and C is group 1; no subgroup was reset. */
    moq_media_sender_test_pump(f->sender, f->local, 1005);
    CHECK(moq_simpair_run_until_quiescent(f->pair, 16, NULL) == MOQ_OK);
    uint64_t groups[4]; unsigned n = 0;
    while (moq_session_poll_events(f->peer, &e, 1)) {
        if (e.kind == MOQ_EVENT_OBJECT_RECEIVED && n < 4)
            groups[n++] = e.u.object_received.group_id;
        moq_event_cleanup(&e);
    }
    CHECK(n == 2);
    if (n == 2) CHECK(groups[1] == groups[0] + 1);
    after = stats(f);
    CHECK(after.objects_sent == 3 && after.objects_queued == 0 &&
          after.objects_dropped == 0 && after.groups_abandoned == 0);
    CHECK(moq_rcbuf_refcount(b) == 1 && moq_rcbuf_refcount(c) == 1);
    /* A valid write is then accepted and sent. */
    CHECK(typed_write(f, false) == MOQ_OK);
    moq_media_sender_test_pump(f->sender, f->local, 1006);
    CHECK(moq_simpair_run_until_quiescent(f->pair, 16, NULL) == MOQ_OK);
    drain_events(f->peer);
    CHECK(stats(f).objects_sent == 4 && stats(f).groups_abandoned == 0 &&
          stats(f).objects_dropped == 0);
done:
    moq_rcbuf_decref(a); moq_rcbuf_decref(b); moq_rcbuf_decref(c);
    moq_rcbuf_decref(incoming);
    scaled_finish(&x);
}

int main(void)
{
    configuration_controls();
    mismatch(MOQ_VERSION_DRAFT_22,4,0);
    mismatch(MOQ_VERSION_DRAFT_22,8,0);
    mismatch(MOQ_VERSION_DRAFT_16,4,1);
    mismatch(MOQ_VERSION_DRAFT_18,4,1);
    wire_control(MOQ_VERSION_DRAFT_16,0,0,MOQ_MEDIA_PACKAGING_RAW,false);
    wire_control(MOQ_VERSION_DRAFT_18,3,UINT32_MAX,MOQ_MEDIA_PACKAGING_RAW,false);
    wire_control(MOQ_VERSION_DRAFT_18,8,0,MOQ_MEDIA_PACKAGING_CMAF,false);
    wire_control(MOQ_VERSION_DRAFT_22,4,1,MOQ_MEDIA_PACKAGING_CMAF,false);
    wire_control(MOQ_VERSION_DRAFT_22,4,1,MOQ_MEDIA_PACKAGING_RAW,false);
    wire_control(MOQ_VERSION_DRAFT_22,8,1,MOQ_MEDIA_PACKAGING_RAW,true);
    for (unsigned i = 0; i < 15; ++i) refusal(i);
    allocation_refusal(false,1);
    allocation_refusal(true,1);
    allocation_refusal(true,2);
    timeout_refusal();
    for (unsigned i = 0; i < 4; ++i) time_endpoints(i);
    group_ids(true);
    group_ids(false);
    for (unsigned i = 0; i < 7; ++i) group_base_refusal(i);
    group_id_ceiling();
    catalog_last_group();
    unobserved_and_future_metadata();
    drop_properties(MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME);
    drop_properties(MOQ_MEDIA_SEND_BP_DROP_GROUP);
    for (unsigned i = EXIT_INTERRUPT; i <= EXIT_WAIT; ++i) blocking_properties((enum blocking_exit)i);
    const moq_version_t drafts[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    for (size_t i = 0; i < 3; ++i)
        for (unsigned v = 0; v < 4; ++v) {
            auto_resolution(drafts[i], v & 1u, (v & 2u) != 0, false, 17);
            /* Presentation time only, from 0 (a media_send-style writer) and
             * mid-stream. */
            if (!(v & 2u)) {
                auto_resolution(drafts[i], v & 1u, false, true, 0);
                auto_resolution(drafts[i], v & 1u, false, true, 33);
            }
        }
    auto_cmaf_without_init(MOQ_VERSION_DRAFT_18);
    auto_cmaf_without_init(MOQ_VERSION_DRAFT_22);
    scale_offer_matrix();
    scale_detector();
    scale_metadata_boundaries();
    scale_refusal_owns_nothing();
    printf("sender_companion rows=%u failures=%u\n",rows,failures);
    return failures ? 1 : 0;
}
#endif
