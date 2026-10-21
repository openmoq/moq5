/* moq_media_player over a real publisher and SimPair, on drafts 16, 18 and
 * 22: the player attached to a receiver that runs on a bare endpoint, the
 * receiver pumped by the test, the player ticked on the test's clock.
 *
 * Pinned:
 *   - a selected RAW/LOC video track comes out as TIMELINE, CONFIGURE, then
 *     its units in decode order, each with its timestamps and payload, and
 *     STARTED once; END follows the last unit, and ENDED;
 *   - END_OF_TRACK with every unit present is END{PEER_END_OF_TRACK,
 *     COMPLETE}, in either order with a normal done, the terminal one
 *     included; a missing unit before it is TAIL_CUTOFF; an error done is
 *     END{PEER_DONE, TAIL_CUTOFF} after the tail wait; a track its receiver
 *     ended for good without END_OF_TRACK -- a terminal normal done, or a
 *     VOD re-subscription the peer refused -- is END{PEER_DONE,
 *     TAIL_CUTOFF} once the tail wait passes, never before, while a re-armed
 *     live subscription is no end; a closed receiver drains what it holds
 *     into END{CLOSED, IMMEDIATE}, then CLOSED;
 *   - AUTO selects the lowest render group, one track per alternate group,
 *     on one timeline; without render groups, one video track; a group in
 *     application media time needs the shared-epoch assertion, or
 *     ALLOW_INDEPENDENT; the others are TRACK_AVAILABLE;
 *   - selection refuses what this release does not play, and a track that
 *     would join a timeline it cannot share;
 *   - an object the engine has no room for is held, in order, and goes in
 *     once outputs are polled; a full output queue is WOULD_BLOCK, nothing
 *     lost; an object larger than the engine is dropped OVERSIZE once;
 *   - DECODER_QUEUE pauses release, and wait then returns DONE at once;
 *     stale feedback is ignored and counted; DECODE_ERROR drops the
 *     track's queued units behind a DISCONTINUITY;
 *   - wait never advances: OK for outputs and due deadlines, INTERRUPTED
 *     with the latch set;
 *   - attach refuses a receiver the player cannot own; outputs are never
 *     truncated; cleanup works after destroy.
 */
#include <moq/media_player.h>
#include <moq/publisher.h>
#include <moq/sim.h>
#include <moq/loc.h>

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

moq_media_receiver_t *moq_media_receiver_test_new_cfg(const moq_media_receiver_cfg_t *cfg);
void moq_media_receiver_test_pump(moq_media_receiver_t *r, moq_session_t *session,
                                  uint64_t now_us);
void moq_media_receiver_test_free(moq_media_receiver_t *r);
void moq_media_receiver_test_destroy_with_session(moq_media_receiver_t *r, moq_session_t *session,
                                                  uint64_t now_us);
void moq_media_receiver_test_set_endpoint(moq_media_receiver_t *r, moq_endpoint_t *ep);
moq_endpoint_t *moq_endpoint_test_make_bare(void);
void moq_endpoint_test_free_bare(moq_endpoint_t *ep);
void moq_endpoint_test_set_stopped(moq_endpoint_t *ep);
void moq_endpoint_test_set_waitable(moq_endpoint_t *ep);
void moq_media_receiver_test_set_fatal(moq_media_receiver_t *r, uint64_t code);
void moq_media_receiver_test_fail_track(moq_media_receiver_t *r, moq_media_track_t *track);
void moq_media_receiver_test_fail_delivery(moq_media_receiver_t *r, moq_media_track_t *track,
                                           bool fill);
bool moq_media_receiver_test_end_of_track(moq_media_receiver_t *r, moq_media_track_t *track,
                                          uint64_t group, uint64_t object);
bool moq_media_receiver_test_inject_object(moq_media_receiver_t *r, moq_media_track_t *track,
                                           uint64_t group, uint64_t object, bool keyframe,
                                           uint64_t time_us, moq_rcbuf_t *payload);

moq_playback_t *moq_media_player_test_engine(const moq_media_player_t *p);
void moq_playback_test_local_issuer(moq_playback_t *pb, uint64_t remaining);
size_t moq_playback_test_buffered_count(const moq_playback_t *pb);

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

#define TS0 UINT64_C(1700000000000000)
#define FRAME_US UINT64_C(33333)
#define TAIL_US UINT64_C(50000)
#define GAP_US UINT64_C(5000)

typedef struct {
    const char *name;
    moq_pub_track_t *pub;
    moq_media_track_t *track;
} pf_track_t;

typedef struct {
    moq_version_t        ver;
    moq_simpair_t       *sp;
    moq_session_t       *rx;
    moq_publisher_t     *pub;
    moq_pub_track_t     *catalog;
    pf_track_t           t[4];
    size_t               n;
    moq_media_receiver_t *r;
    moq_endpoint_t      *ep;
    moq_media_player_t  *p;
    uint64_t             now;
    bool                 ready;
} pf_t;

typedef struct {
    uint32_t selection;
    uint32_t timing_policy;
    bool     assume_shared_epoch;
    uint32_t max_outputs;
    uint32_t max_events;
    uint32_t max_buffered_objects;
    uint64_t max_buffered_bytes;
    uint64_t gap_timeout_us;
    uint64_t max_held_bytes;
    const char *lang;
    uint32_t max_track_events;
    uint32_t engine_max_events;
    uint32_t engine_max_known_ranges;
    uint32_t max_stream_terminals;
} pf_opts_t;

static moq_media_track_t *pf_track(pf_t *f, const char *name)
{
    for (size_t i = 0; i < f->n; i++)
        if (strcmp(f->t[i].name, name) == 0) return f->t[i].track;
    return NULL;
}

static moq_pub_track_t *pf_pub(pf_t *f, const char *name)
{
    for (size_t i = 0; i < f->n; i++)
        if (strcmp(f->t[i].name, name) == 0) return f->t[i].pub;
    return NULL;
}

/* One step of the network, the receiver and the player. */
static moq_result_t cycle(pf_t *f)
{
    f->now += 1000;
    CHECK(moq_simpair_advance_to(f->sp, f->now) == MOQ_OK);
    CHECK(moq_simpair_run_until_quiescent(f->sp, 64, NULL) >= 0);
    if (f->pub) CHECK(moq_pub_tick(f->pub, f->now) >= 0);
    CHECK(moq_session_process_pending(f->rx, f->now) >= 0);
    moq_media_receiver_test_pump(f->r, f->rx, f->now);
    return moq_media_player_tick(f->p, f->now);
}

static void cycles(pf_t *f, unsigned n)
{
    for (unsigned i = 0; i < n; i++) (void)cycle(f);
}

/* Learn the receiver's handles from the descriptor names it announced. */
static void pf_learn(pf_t *f, moq_media_track_t *track)
{
    const moq_media_track_desc_t *d = moq_media_track_desc_get(track);
    if (!d) return;
    for (size_t i = 0; i < f->n; i++)
        if (d->name.len == strlen(f->t[i].name) &&
            memcmp(d->name.data, f->t[i].name, d->name.len) == 0)
            f->t[i].track = track;
}

typedef struct {
    moq_media_player_event_t ev[64];
    size_t n;
} ev_log_t;

static void events(pf_t *f, ev_log_t *log)
{
    moq_media_player_event_t ev;
    while (moq_media_player_poll_event(f->p, &ev, sizeof(ev)) == MOQ_OK) {
        if (ev.track) pf_learn(f, ev.track);
        if (ev.kind == MOQ_MEDIA_PLAYER_EVENT_READY) f->ready = true;
        if (log && log->n < 64) log->ev[log->n++] = ev;
    }
}

static size_t count_ev(const ev_log_t *log, uint32_t kind)
{
    size_t n = 0;
    for (size_t i = 0; i < log->n; i++) n += log->ev[i].kind == kind;
    return n;
}

static bool pf_setup(pf_t *f, moq_version_t ver, const char *catalog,
                     const char *const *names, size_t n, const pf_opts_t *o)
{
    memset(f, 0, sizeof(*f));
    f->ver = ver;
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = ver;
    cfg.seed = 0x9a7e;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 64;
    if (!CHECK(moq_simpair_create(&cfg, &f->sp) == MOQ_OK)) return false;
    if (!CHECK(moq_simpair_start(f->sp) == MOQ_OK)) return false;
    f->rx = moq_simpair_server(f->sp);
    for (unsigned i = 0; i < 8; i++) {
        f->now += 1000;
        moq_simpair_advance_to(f->sp, f->now);
        moq_simpair_run_until_quiescent(f->sp, 64, NULL);
    }
    if (!CHECK(moq_session_state(f->rx) == MOQ_SESS_ESTABLISHED)) return false;

    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    if (!CHECK(moq_pub_create(moq_simpair_client(f->sp), moq_alloc_default(),
                              &pcfg, &f->pub) == MOQ_OK)) return false;
    static moq_bytes_t parts[] = { { (const uint8_t *)"play", 4 },
                                   { (const uint8_t *)"test", 4 } };
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init_sized(&tcfg, sizeof(tcfg));
    tcfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 2 };
    tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
    if (!CHECK(moq_pub_add_track(f->pub, &tcfg, f->now, &f->catalog) == MOQ_OK))
        return false;
    for (size_t i = 0; i < n; i++) {
        f->t[i].name = names[i];
        tcfg.track_name = (moq_bytes_t){ (const uint8_t *)names[i], strlen(names[i]) };
        if (!CHECK(moq_pub_add_track(f->pub, &tcfg, f->now, &f->t[i].pub) == MOQ_OK))
            return false;
    }
    f->n = n;
    moq_rcbuf_t *cat = NULL;
    moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)catalog, strlen(catalog), &cat);
    moq_pub_retained_object_t retained = {
        .object_id = 0, .payload = cat, .properties = NULL, .end_of_group = true };
    moq_pub_retained_group_cfg_t group;
    moq_pub_retained_group_cfg_init(&group);
    group.objects = &retained;
    group.object_count = 1;
    moq_result_t crc = moq_pub_set_retained_group(f->pub, f->catalog, &group);
    moq_rcbuf_decref(cat);
    if (!CHECK(crc == MOQ_OK)) return false;

    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_sized(&rcfg, sizeof(rcfg));
    rcfg.namespace_ = tcfg.track_namespace;
    rcfg.auto_subscribe = false;
    rcfg.time_mode = MOQ_MEDIA_TIME_RAW;
    rcfg.overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
    rcfg.overflow.max_objects = 64;
    rcfg.overflow.max_bytes = 1u << 20;
    rcfg.surface_stream_terminals = true;
    if (o && o->max_track_events) rcfg.max_track_events = o->max_track_events;
    if (o && o->max_stream_terminals) rcfg.max_stream_terminals = o->max_stream_terminals;
    f->r = moq_media_receiver_test_new_cfg(&rcfg);
    if (!CHECK(f->r != NULL)) return false;
    f->ep = moq_endpoint_test_make_bare();
    if (!CHECK(f->ep != NULL)) return false;
    moq_media_receiver_test_set_endpoint(f->r, f->ep);

    moq_playback_cfg_t pb;
    moq_playback_cfg_init(&pb);
    pb.gap_timeout_us = o && o->gap_timeout_us ? o->gap_timeout_us : GAP_US;
    pb.max_backlog_groups = 0;
    if (o && o->max_buffered_objects) pb.max_buffered_objects = o->max_buffered_objects;
    if (o && o->max_buffered_bytes) pb.max_buffered_bytes = o->max_buffered_bytes;
    if (o && o->engine_max_events) pb.max_events = o->engine_max_events;
    if (o && o->engine_max_known_ranges) pb.max_known_ranges = o->engine_max_known_ranges;
    moq_media_player_cfg_t c;
    moq_media_player_cfg_init(&c);
    c.selection = o && o->selection ? o->selection : MOQ_MEDIA_SELECTION_MANUAL;
    c.timing_policy = o ? o->timing_policy : 0;
    c.assume_shared_epoch = o && o->assume_shared_epoch;
    c.max_outputs = o ? o->max_outputs : 0;
    c.max_events = o ? o->max_events : 0;
    c.max_held_bytes = o ? o->max_held_bytes : 0;
    if (o && o->lang)
        c.preferred_lang = (moq_bytes_t){ (const uint8_t *)o->lang, strlen(o->lang) };
    c.playback = &pb;
    c.end_tail_wait_us = TAIL_US;
    if (!CHECK(moq_media_player_attach(f->r, &c, &f->p) == MOQ_OK)) return false;
    for (unsigned i = 0; i < 64 && !f->ready; i++) {
        (void)cycle(f);
        events(f, NULL);
    }
    return CHECK(f->ready);
}

static void pf_teardown(pf_t *f)
{
    if (f->p) moq_media_player_destroy(f->p);
    if (f->r) {
        /* Its subscriber facade goes through the session, as the endpoint
         * tears it down; freeing the receiver alone would leave it. */
        moq_media_receiver_test_set_endpoint(f->r, NULL);
        moq_media_receiver_test_destroy_with_session(f->r, f->rx, f->now);
    }
    if (f->ep) moq_endpoint_test_free_bare(f->ep);
    if (f->pub) moq_pub_destroy(f->pub);
    if (f->sp) moq_simpair_destroy(f->sp);
}

static uint64_t ts_of(uint64_t g, uint64_t o)
{
    return TS0 + (g * 30 + o) * FRAME_US;
}

/* A LOC object of track `name` at (g, o): its timestamp, and for video its
 * marking. With `timescale`, a draft-22 Timescale (media time). With
 * `eog`, its subgroup ends its group: the group's FIN then says no later
 * object exists. */
static void write_loc_eog(pf_t *f, const char *name, uint64_t g, uint64_t o,
                          bool independent, uint64_t timescale, size_t size,
                          bool eog)
{
    moq_loc_metadata_t md;
    moq_loc_metadata_init_sized(&md, sizeof(md));
    md.present = MOQ_LOC_METADATA_HAS_TIMESTAMP | MOQ_LOC_METADATA_HAS_MARKING;
    md.timestamp = timescale ? (g * 30 + o) * 3000 : ts_of(g, o);
    if (timescale) {
        md.present |= MOQ_LOC_METADATA_HAS_TIMESCALE;
        md.timescale = timescale;
    }
    md.marking_len = 1;
    md.marking[0] = independent ? 0xE0 : 0xC0;
    moq_rcbuf_t *props = NULL;
    moq_loc_profile_t prof = f->ver == MOQ_VERSION_DRAFT_22 ? MOQ_LOC_PROFILE_04
                                                            : MOQ_LOC_PROFILE_01;
    CHECK(moq_loc_encode_metadata(moq_alloc_default(), f->ver, prof, &md, &props) == MOQ_OK);
    uint8_t big[4096];
    memset(big, (int)(g * 16 + o), sizeof(big));
    if (size > sizeof(big)) size = sizeof(big);
    if (size == 0) size = 8;
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(moq_alloc_default(), big, size, &pay);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.object_id = o;
    oc.payload = pay;
    oc.properties = props;
    oc.end_of_group = eog;
    CHECK(moq_pub_write_object_ex(f->pub, pf_pub(f, name), &oc, f->now) == MOQ_OK);
    moq_rcbuf_decref(pay);
    moq_rcbuf_decref(props);
}

static void write_loc(pf_t *f, const char *name, uint64_t g, uint64_t o,
                      bool independent, uint64_t timescale, size_t size)
{
    write_loc_eog(f, name, g, o, independent, timescale, size, false);
}

static void end_group(pf_t *f, const char *name)
{
    CHECK(moq_pub_end_group(f->pub, pf_pub(f, name), f->now) == MOQ_OK);
}

typedef struct {
    moq_media_player_output_t o[96];
    size_t n;
} out_log_t;

/* Poll every output into the log, returning their refs. */
static void outputs(pf_t *f, out_log_t *log)
{
    moq_media_player_output_t o;
    while (moq_media_player_poll_output(f->p, &o, sizeof(o)) == MOQ_OK) {
        if (o.track) pf_learn(f, o.track);
        if (log && log->n < 96) {
            log->o[log->n] = o;
            log->o[log->n].u.unit.payload = NULL;
            if (o.kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT && o.u.unit.payload)
                log->o[log->n].u.unit.len = moq_rcbuf_len(o.u.unit.payload);
            if (o.kind == MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE) {
                log->o[log->n].u.configure.codec = NULL;
                log->o[log->n].u.configure.codec_config = NULL;
            }
            log->n++;
        }
        moq_media_player_output_cleanup(&o);
    }
}

static size_t count_out(const out_log_t *log, uint32_t kind, moq_media_track_t *t)
{
    size_t n = 0;
    for (size_t i = 0; i < log->n; i++)
        n += log->o[i].kind == kind && (!t || log->o[i].track == t);
    return n;
}

static const moq_media_player_output_t *last_out(const out_log_t *log, uint32_t kind)
{
    for (size_t i = log->n; i > 0; i--)
        if (log->o[i - 1].kind == kind) return &log->o[i - 1];
    return NULL;
}

static const char *vname(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_22 ? "d22" : v == MOQ_VERSION_DRAFT_18 ? "d18" : "d16";
}

#define LOC_VIDEO(name, extra) \
    "{\"name\":\"" name "\",\"packaging\":\"loc\",\"isLive\":true," \
    "\"role\":\"video\",\"codec\":\"avc1.42e01e\"" extra "}"
#define LOC_AUDIO(name, codec, extra) \
    "{\"name\":\"" name "\",\"packaging\":\"loc\",\"isLive\":true," \
    "\"role\":\"audio\",\"codec\":\"" codec "\",\"samplerate\":48000," \
    "\"channelConfig\":\"2\"" extra "}"
#define CATALOG(tracks) "{\"version\":1,\"tracks\":[" tracks "]}"

static const char *const k_v[] = { "video" };
static const char k_cat_v[] = CATALOG(LOC_VIDEO("video", ""));

static moq_result_t select_plain(pf_t *f, const char *name)
{
    moq_media_track_t *t = pf_track(f, name);
    if (!t) return MOQ_ERR_INVAL;
    return moq_media_player_select_track(f->p, t, NULL);
}

/* The publisher ends the track's subscription for good with a normal done:
 * draft 22's first TRACK_ENDED re-arms it for VOD, so the done that ends
 * it is the next one; drafts 16/18 end it with SUBSCRIPTION_ENDED. */
static void terminal_normal_done(pf_t *f, moq_version_t ver, const char *name)
{
    if (ver == MOQ_VERSION_DRAFT_22) {
        CHECK(moq_pub_finish_subscribers(f->pub, pf_pub(f, name), 0x2u, f->now) == MOQ_OK);
        cycles(f, 24);
        CHECK(moq_pub_finish_subscribers(f->pub, pf_pub(f, name), 0x2u, f->now) == MOQ_OK);
    } else {
        CHECK(moq_pub_finish_subscribers(f->pub, pf_pub(f, name), 0x3u, f->now) == MOQ_OK);
    }
}

/* Cycles until the receiver reports `name` ENDED (bounded). */
static bool until_receiver_ended(pf_t *f, const char *name)
{
    for (unsigned i = 0; i < 64; i++) {
        moq_media_track_state_t state;
        if (moq_media_receiver_track_state(f->r, pf_track(f, name), &state) == MOQ_OK &&
            state == MOQ_MEDIA_TRACK_STATE_ENDED)
            return true;
        (void)cycle(f);
    }
    return false;
}

/* One cycle with the network, the receiver and the player at caller time
 * `t` (at least a microsecond ahead). */
static moq_result_t cycle_to(pf_t *f, uint64_t t)
{
    f->now = t - 1000;
    return cycle(f);
}

/* After the receiver ended the track for good: no END before the tail
 * deadline, then exactly one END{PEER_DONE, TAIL_CUTOFF} at it -- the
 * boundary pinned at explicit caller times, a microsecond before and at. */
static void expect_bounded_peer_done(pf_t *f)
{
    out_log_t log = {0};
    outputs(f, &log);
    uint64_t deadline = moq_media_player_next_deadline_us(f->p);
    CHECK(deadline != UINT64_MAX && deadline <= f->now + TAIL_US);
    if (deadline == UINT64_MAX || deadline <= f->now + 1000) return;
    (void)cycle_to(f, deadline - 1);
    outputs(f, &log);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, NULL) == 0);   /* before */
    (void)cycle_to(f, deadline);
    outputs(f, &log);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, NULL) == 1);   /* at it */
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    if (e) {
        CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_DONE);
        CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF);
    }
    ev_log_t evl = {0};
    events(f, &evl);
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_ENDED) == 1);
    for (unsigned i = 0; i < 40; i++) {
        (void)cycle(f);
        outputs(f, &log);
    }
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, NULL) == 1);   /* once */
}

/* A video track in decode order: TIMELINE before the first unit, then
 * CONFIGURE, then every unit with its timestamps and bytes; END_OF_TRACK
 * after them all is a COMPLETE end, then ENDED. */
static void test_video_vertical(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "video_vertical_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    for (uint64_t o = 0; o < 3; o++) write_loc_eog(&f, "video", 0, o, o == 0, 0, 100 + o, true);
    end_group(&f, "video");
    write_loc_eog(&f, "video", 1, 0, true, 0, 50, true);
    end_group(&f, "video");
    cycles(&f, 16);
    CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
    cycles(&f, 16);
    out_log_t log = {0};
    ev_log_t evl = {0};
    outputs(&f, &log);
    events(&f, &evl);
    moq_media_track_t *v = pf_track(&f, "video");
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_TIMELINE, v) == 1);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, v) == 4);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, v) == 1);
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_STARTED) == 1);
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_ENDED) == 1);
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_GAP) == 0);
    /* Order: TIMELINE and CONFIGURE ahead of the first unit, END last. */
    size_t first_unit = SIZE_MAX, tl = SIZE_MAX, cfg = SIZE_MAX;
    uint64_t g[4], o[4], dts[4];
    size_t len[4], k = 0;
    for (size_t i = 0; i < log.n; i++) {
        const moq_media_player_output_t *x = &log.o[i];
        CHECK(x->struct_size == sizeof(*x));
        CHECK(x->timeline_id != 0);
        if (x->kind == MOQ_MEDIA_PLAYER_OUTPUT_TIMELINE && tl == SIZE_MAX) tl = i;
        if (x->kind == MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE && cfg == SIZE_MAX) cfg = i;
        if (x->kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT) {
            if (first_unit == SIZE_MAX) first_unit = i;
            if (k < 4) {
                g[k] = x->u.unit.group_id;
                o[k] = x->u.unit.object_id;
                dts[k] = x->u.unit.decode_time_us;
                len[k] = x->u.unit.len;
                k++;
            }
            CHECK(x->u.unit.present);
            CHECK(x->u.unit.presentation_time_us == x->u.unit.decode_time_us);
        }
    }
    CHECK(tl < first_unit && cfg < first_unit);
    CHECK(log.n > 0 && log.o[log.n - 1].kind == MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(k == 4);
    CHECK(g[0] == 0 && o[0] == 0 && g[1] == 0 && o[1] == 1 && g[2] == 0 && o[2] == 2);
    CHECK(g[3] == 1 && o[3] == 0);
    CHECK(len[0] == 100 && len[1] == 101 && len[2] == 102 && len[3] == 50);
    CHECK(dts[1] == dts[0] + FRAME_US && dts[3] > dts[2]);
    const moq_media_player_output_t *t = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_TIMELINE);
    REQUIRE(t != NULL);
    CHECK(t->u.timeline.anchor_media_us == dts[0]);
    CHECK(t->u.timeline.rate == 1.0);
    CHECK(t->u.timeline.domain == MOQ_MEDIA_TIME_DOMAIN_UNIX_US);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_COMPLETE);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A unit missing when END_OF_TRACK comes: the engine stops waiting on it
 * after the end, so the end is a cutoff, never COMPLETE. */
static void test_end_with_hole(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "end_with_hole_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    write_loc(&f, "video", 0, 2, false, 0, 0);
    end_group(&f, "video");
    CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
    for (unsigned i = 0; i < 80; i++) (void)cycle(&f);
    out_log_t log = {0};
    outputs(&f, &log);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* An error done: the tail wait runs, then END{PEER_DONE, TAIL_CUTOFF}. A
 * normal done that ends the track for good, with no END_OF_TRACK, is the
 * same END once the tail wait passes. */
static void test_done(moq_version_t ver, bool error)
{
    static char label[64];
    snprintf(label, sizeof(label), "done_%s_%s", error ? "error" : "normal", vname(ver));
    row = label;
    unsigned before = failures;
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    end_group(&f, "video");
    cycles(&f, 8);
    if (error) {
        CHECK(moq_pub_finish_subscribers(f.pub, pf_pub(&f, "video"), 0x4u, f.now) == MOQ_OK);
    } else {
        /* A normal done that ends the track for good, without END_OF_TRACK:
         * bounded by the tail wait, never COMPLETE. */
        terminal_normal_done(&f, ver, "video");
        REQUIRE(until_receiver_ended(&f, "video"));
        expect_bounded_peer_done(&f);
        goto out;
    }
    cycles(&f, 8);
    out_log_t log = {0};
    outputs(&f, &log);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, NULL) == 0);
    if (error) CHECK(moq_media_player_next_deadline_us(f.p) <= f.now + TAIL_US);
    for (unsigned i = 0; i < 80; i++) (void)cycle(&f);
    outputs(&f, &log);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    if (error) {
        REQUIRE(e != NULL);
        CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_DONE);
        CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF);
    } else {
        CHECK(e == NULL);
    }
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* The receiver closes with units held: they are released, then
 * END{CLOSED, IMMEDIATE}, CLOSED, and the queues report CLOSED. */
static void test_receiver_closed(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "receiver_closed_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    write_loc(&f, "video", 0, 1, false, 0, 0);
    write_loc(&f, "video", 0, 3, false, 0, 0);
    cycles(&f, 8);
    moq_endpoint_test_set_stopped(f.ep);
    cycles(&f, 8);
    out_log_t log = {0};
    ev_log_t evl = {0};
    outputs(&f, &log);
    events(&f, &evl);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, NULL) == 2);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_CLOSED);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_IMMEDIATE);
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_CLOSED) == 1);
    CHECK(moq_media_player_is_closed(f.p));
    moq_media_player_output_t o;
    CHECK(moq_media_player_poll_output(f.p, &o, sizeof(o)) == MOQ_ERR_CLOSED);
    CHECK(moq_media_player_tick(f.p, f.now) == MOQ_ERR_CLOSED);
    CHECK(moq_media_player_wait(f.p, f.now, 1000) == MOQ_ERR_CLOSED);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* AUTO: the lowest render group, one per alternate group (the middle
 * bitrate), on one timeline; the rest are available. */
static void test_auto_group(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "auto_group_%s", vname(ver));
    row = label;
    unsigned before = failures;
    static const char cat[] = CATALOG(
        LOC_VIDEO("hi", ",\"renderGroup\":1,\"altGroup\":1,\"bitrate\":3000") ","
        LOC_VIDEO("md", ",\"renderGroup\":1,\"altGroup\":1,\"bitrate\":2000") ","
        LOC_VIDEO("lo", ",\"renderGroup\":1,\"altGroup\":1,\"bitrate\":1000") ","
        LOC_AUDIO("audio", "opus", ",\"renderGroup\":1") ","
        LOC_VIDEO("other", ",\"renderGroup\":2"));
    static const char *const names[] = { "hi", "md", "lo", "audio" };
    pf_opts_t o = { .selection = MOQ_MEDIA_SELECTION_AUTO };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, cat, names, 4, &o));
    moq_media_player_stats_t st;
    CHECK(moq_media_player_get_stats(f.p, &st, sizeof(st)) == MOQ_OK);
    CHECK(st.tracks_selected == 2);
    cycles(&f, 16);
    write_loc(&f, "md", 0, 0, true, 0, 0);
    write_loc(&f, "audio", 0, 0, true, 0, 0);
    write_loc(&f, "hi", 0, 0, true, 0, 0);
    cycles(&f, 16);
    out_log_t log = {0};
    outputs(&f, &log);
    moq_media_track_t *md = pf_track(&f, "md"), *au = pf_track(&f, "audio");
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, md) == 1);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, au) == 1);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, pf_track(&f, "hi")) == 0);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_TIMELINE, NULL) == 1);
    uint32_t tl_md = 0, tl_au = 0;
    for (size_t i = 0; i < log.n; i++) {
        if (log.o[i].track == md) tl_md = log.o[i].timeline_id;
        if (log.o[i].track == au) tl_au = log.o[i].timeline_id;
    }
    CHECK(tl_md != 0 && tl_md == tl_au);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* AUTO without render groups plays one video track; the others are
 * available. */
static void test_auto_ungrouped(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "auto_ungrouped_%s", vname(ver));
    row = label;
    unsigned before = failures;
    static const char cat[] = CATALOG(
        LOC_AUDIO("audio", "opus", "") "," LOC_VIDEO("video", ""));
    static const char *const names[] = { "audio", "video" };
    pf_opts_t o = { .selection = MOQ_MEDIA_SELECTION_AUTO };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, cat, names, 2, &o));
    moq_media_player_stats_t st;
    CHECK(moq_media_player_get_stats(f.p, &st, sizeof(st)) == MOQ_OK);
    CHECK(st.tracks_selected == 1);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    write_loc(&f, "audio", 0, 0, true, 0, 0);
    cycles(&f, 16);
    out_log_t log = {0};
    outputs(&f, &log);
    REQUIRE(pf_track(&f, "video") != NULL && pf_track(&f, "audio") != NULL);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, pf_track(&f, "video")) == 1);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, pf_track(&f, "audio")) == 0);
    CHECK(moq_media_player_select_track(f.p, pf_track(&f, "video"), NULL) ==
          MOQ_ERR_WRONG_STATE);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* Draft 22, a render group in application media time: not selected without
 * the shared-epoch assertion (SYNC_UNAVAILABLE); selected on one timeline
 * with it; on independent timelines under ALLOW_INDEPENDENT. */
static void test_auto_media_time(int variant)
{
    static const char *const vn[] = { "require", "shared", "independent" };
    static char label[64];
    snprintf(label, sizeof(label), "auto_media_time_%s", vn[variant]);
    row = label;
    unsigned before = failures;
    static const char cat[] = CATALOG(
        LOC_VIDEO("video", ",\"renderGroup\":1,\"timescale\":90000") ","
        LOC_AUDIO("audio", "opus", ",\"renderGroup\":1,\"timescale\":48000"));
    static const char *const names[] = { "video", "audio" };
    pf_opts_t o = { .selection = MOQ_MEDIA_SELECTION_AUTO };
    if (variant == 1) o.assume_shared_epoch = true;
    if (variant == 2) o.timing_policy = MOQ_MEDIA_TIMING_ALLOW_INDEPENDENT;
    pf_t f;
    ev_log_t evl = {0};
    REQUIRE(pf_setup(&f, MOQ_VERSION_DRAFT_22, cat, names, 2, &o));
    moq_media_player_stats_t st;
    CHECK(moq_media_player_get_stats(f.p, &st, sizeof(st)) == MOQ_OK);
    CHECK(st.tracks_selected == (variant == 0 ? 0u : 2u));
    cycles(&f, 8);
    events(&f, &evl);
    if (variant == 0) {
        /* The events came during setup; the selection says enough. */
        CHECK(moq_media_player_select_track(f.p, pf_track(&f, "video"), NULL) == MOQ_OK);
        moq_media_player_select_cfg_t sc;
        moq_media_player_select_cfg_init(&sc);
        CHECK(moq_media_player_select_track(f.p, pf_track(&f, "audio"), &sc) ==
              MOQ_ERR_UNSUPPORTED);
        sc.independent_timing = true;
        CHECK(moq_media_player_select_track(f.p, pf_track(&f, "audio"), &sc) == MOQ_OK);
        cycles(&f, 16);
    }
    write_loc(&f, "video", 0, 0, true, 90000, 0);
    write_loc(&f, "audio", 0, 0, true, 48000, 0);
    cycles(&f, 16);
    out_log_t log = {0};
    outputs(&f, &log);
    size_t timelines = count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_TIMELINE, NULL);
    CHECK(timelines == (variant == 1 ? 1u : 2u));
    const moq_media_player_output_t *t = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_TIMELINE);
    if (t) CHECK(t->u.timeline.domain == MOQ_MEDIA_TIME_DOMAIN_APPLICATION_MEDIA);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* What this release does not play is refused at selection. */
static void test_unsupported(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "unsupported_%s", vname(ver));
    row = label;
    unsigned before = failures;
    static const char cat[] = CATALOG(
        LOC_AUDIO("vorbis", "vorbis", "") ","
        LOC_AUDIO("aac", "mp4a.40.2", "") ","
        LOC_AUDIO("opus", "opus", "") ","
        "{\"name\":\"ts\",\"packaging\":\"mpeg2ts\",\"isLive\":true,"
        "\"mimeType\":\"video/mp2t\",\"mpeg2tsMode\":\"unmodified-program\","
        "\"mpeg2tsPacketSize\":188,\"mpeg2tsProgramNumber\":1}");
    static const char *const names[] = { "vorbis", "aac", "opus", "ts" };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, cat, names, 4, NULL));
    CHECK(select_plain(&f, "vorbis") == MOQ_ERR_UNSUPPORTED);
    CHECK(select_plain(&f, "ts") == MOQ_ERR_UNSUPPORTED);
    /* AAC needs its configuration: from the catalog, or in band, which only
     * draft 22 carries. */
    CHECK(select_plain(&f, "aac") ==
          (ver == MOQ_VERSION_DRAFT_22 ? MOQ_OK : MOQ_ERR_UNSUPPORTED));
    CHECK(select_plain(&f, "opus") == MOQ_OK);
    CHECK(select_plain(&f, "opus") == MOQ_ERR_WRONG_STATE);
    moq_media_player_select_cfg_t sc;
    moq_media_player_select_cfg_init(&sc);
    sc.start = 7;
    CHECK(moq_media_player_select_track(f.p, pf_track(&f, "vorbis"), &sc) == MOQ_ERR_INVAL);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A full engine holds the next object, in order, and it goes in once
 * outputs are polled; a small output queue is WOULD_BLOCK; nothing is lost
 * and every unit comes out in decode order. */
static void test_backpressure(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "backpressure_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .max_outputs = 3, .max_buffered_objects = 2 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    for (uint64_t ob = 0; ob < 12; ob++)
        write_loc(&f, "video", 0, ob, ob == 0, 0, 0);
    bool blocked = false, held = false;
    uint64_t next = 0;
    for (unsigned i = 0; i < 200 && next < 12; i++) {
        moq_result_t rc = cycle(&f);
        blocked |= rc == MOQ_ERR_WOULD_BLOCK;
        moq_media_player_stats_t st;
        CHECK(moq_media_player_get_stats(f.p, &st, sizeof(st)) == MOQ_OK);
        held |= st.held_objects > 0;
        moq_media_player_output_t x;
        while (moq_media_player_poll_output(f.p, &x, sizeof(x)) == MOQ_OK) {
            if (x.kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT) {
                CHECK(x.u.unit.object_id == next);
                next++;
            }
            moq_media_player_output_cleanup(&x);
        }
    }
    CHECK(next == 12);
    CHECK(blocked);
    CHECK(held);
    moq_media_receiver_stats_t rs;
    CHECK(moq_media_receiver_get_stats(f.r, &rs, sizeof(rs)) == MOQ_OK);
    CHECK(rs.objects_dropped == 0);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* An object larger than the engine's whole budget is dropped OVERSIZE,
 * once; the next one plays. */
static void test_oversize(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "oversize_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .max_buffered_bytes = 1024 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 4000);
    end_group(&f, "video");
    write_loc(&f, "video", 1, 0, true, 0, 100);
    cycles(&f, 24);
    out_log_t log = {0};
    ev_log_t evl = {0};
    outputs(&f, &log);
    events(&f, &evl);
    size_t oversize = 0;
    for (size_t i = 0; i < evl.n; i++)
        oversize += evl.ev[i].kind == MOQ_MEDIA_PLAYER_EVENT_DROP &&
                    evl.ev[i].u.drop.reason == MOQ_MEDIA_PLAYER_DROP_OVERSIZE;
    CHECK(oversize == 1);
    const moq_media_player_output_t *u = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT);
    REQUIRE(u != NULL);
    CHECK(u->u.unit.group_id == 1);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* DECODER_QUEUE above its mark pauses release, and wait returns DONE at
 * once; feedback naming an older generation is ignored; below the mark
 * release resumes. DECODE_ERROR drops the queued units behind a
 * DISCONTINUITY. */
static void test_feedback(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "feedback_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video");
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    cycles(&f, 8);
    out_log_t log = {0};
    outputs(&f, &log);
    const moq_media_player_output_t *u = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT);
    REQUIRE(u != NULL);
    uint64_t gen = u->configuration_generation;
    moq_media_player_feedback_t fb;
    moq_media_player_feedback_init(&fb);
    fb.kind = MOQ_MEDIA_PLAYER_FEEDBACK_DECODER_QUEUE;
    fb.track = v;
    fb.configuration_generation = gen;
    fb.u.decoder_queue.depth = 9;
    fb.u.decoder_queue.max_recommended = 4;
    CHECK(moq_media_player_handle_feedback(f.p, &fb, f.now) == MOQ_OK);
    write_loc(&f, "video", 0, 1, false, 0, 0);
    cycles(&f, 8);
    out_log_t paused = {0};
    outputs(&f, &paused);
    events(&f, NULL);
    CHECK(count_out(&paused, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, v) == 0);
    /* Repeated wait and tick: DONE at once each time, no unit, no spin. */
    for (int i = 0; i < 3; i++) {
        CHECK(moq_media_player_wait(f.p, f.now, 1000000) == MOQ_DONE);
        CHECK(moq_media_player_tick(f.p, f.now) == MOQ_OK);
        out_log_t again = {0};
        outputs(&f, &again);
        events(&f, NULL);
        CHECK(count_out(&again, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, v) == 0);
    }
    moq_media_player_feedback_t stale = fb;
    stale.configuration_generation = gen + 7;
    stale.u.decoder_queue.depth = 0;
    CHECK(moq_media_player_handle_feedback(f.p, &stale, f.now) == MOQ_OK);
    moq_media_player_stats_t st;
    CHECK(moq_media_player_get_stats(f.p, &st, sizeof(st)) == MOQ_OK);
    CHECK(st.feedback_stale == 1);
    fb.u.decoder_queue.depth = 0;
    CHECK(moq_media_player_handle_feedback(f.p, &fb, f.now) == MOQ_OK);
    cycles(&f, 4);
    out_log_t resumed = {0};
    outputs(&f, &resumed);
    CHECK(count_out(&resumed, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, v) == 1);

    /* A decode error: units still queued are dropped, a DISCONTINUITY
     * follows, and the track waits for its next independent unit. */
    write_loc(&f, "video", 0, 2, false, 0, 0);
    cycles(&f, 8);
    moq_media_player_feedback_t err;
    moq_media_player_feedback_init(&err);
    err.kind = MOQ_MEDIA_PLAYER_FEEDBACK_DECODE_ERROR;
    err.track = v;
    err.configuration_generation = gen;
    CHECK(moq_media_player_handle_feedback(f.p, &err, f.now) == MOQ_OK);
    write_loc(&f, "video", 0, 3, false, 0, 0);
    end_group(&f, "video");
    write_loc(&f, "video", 1, 0, true, 0, 0);
    cycles(&f, 16);
    out_log_t after = {0};
    outputs(&f, &after);
    CHECK(count_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, v) == 1);
    const moq_media_player_output_t *d = NULL;
    for (size_t i = 0; i < after.n && !d; i++)
        if (after.o[i].kind == MOQ_MEDIA_PLAYER_OUTPUT_DISCONTINUITY) d = &after.o[i];
    REQUIRE(d != NULL);
    CHECK(d->u.discontinuity.reason == MOQ_MEDIA_PLAYER_DISCONTINUITY_DECODE_ERROR);
    CHECK(d->configuration_generation > gen);
    /* The unit queued before the error was dropped with it. */
    for (size_t i = 0; i < after.n; i++)
        if (after.o[i].kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT)
            CHECK(after.o[i].u.unit.object_id != 2);
    const moq_media_player_output_t *u2 = last_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_UNIT);
    if (u2) {
        CHECK(u2->u.unit.group_id == 1);
        CHECK(u2->configuration_generation > gen);
    }
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* wait never advances: OK while an output waits, OK for a due deadline,
 * INTERRUPTED with the latch set. */
static void test_wait(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "wait_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    events(&f, NULL);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    cycles(&f, 8);
    moq_media_player_stats_t a, b;
    CHECK(moq_media_player_get_stats(f.p, &a, sizeof(a)) == MOQ_OK);
    CHECK(moq_media_player_wait(f.p, f.now, 1000) == MOQ_OK);
    CHECK(moq_media_player_wait(f.p, f.now, 1000) == MOQ_OK);
    CHECK(moq_media_player_get_stats(f.p, &b, sizeof(b)) == MOQ_OK);
    CHECK(a.outputs_queued == b.outputs_queued && a.outputs_queued > 0);
    outputs(&f, NULL);
    events(&f, NULL);
    /* The error done arms the tail wait: a deadline, OK once it is due. */
    CHECK(moq_pub_finish_subscribers(f.pub, pf_pub(&f, "video"), 0x4u, f.now) == MOQ_OK);
    cycles(&f, 4);
    outputs(&f, NULL);
    events(&f, NULL);
    uint64_t d = moq_media_player_next_deadline_us(f.p);
    CHECK(d != UINT64_MAX && d > f.now);
    CHECK(moq_media_player_wait(f.p, d, 1000) == MOQ_OK);
    moq_endpoint_set_interrupted(f.ep, true);
    CHECK(moq_media_player_wait(f.p, f.now, 1000) == MOQ_ERR_INTERRUPTED);
    moq_endpoint_set_interrupted(f.ep, false);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* Attach takes only a receiver it can own; the record and argument
 * contracts. */
static void test_contracts(void)
{
    row = "contracts";
    unsigned before = failures;
    moq_media_player_cfg_t c;
    moq_media_player_cfg_init(&c);
    CHECK(c.struct_size == sizeof(c));
    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_sized(&rcfg, sizeof(rcfg));
    static moq_bytes_t parts[] = { { (const uint8_t *)"x", 1 } };
    rcfg.namespace_ = (moq_namespace_t){ parts, 1 };
    rcfg.overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
    rcfg.auto_subscribe = false;
    rcfg.surface_stream_terminals = true;
    moq_media_player_t *p = NULL;
    moq_media_receiver_t *r = moq_media_receiver_test_new_cfg(&rcfg);
    REQUIRE(r != NULL);
    CHECK(moq_media_player_attach(NULL, &c, &p) == MOQ_ERR_INVAL);
    c.receiver = &rcfg;
    CHECK(moq_media_player_attach(r, &c, &p) == MOQ_ERR_INVAL);
    c.receiver = NULL;
    c.selection = 9;
    CHECK(moq_media_player_attach(r, &c, &p) == MOQ_ERR_INVAL);
    c.selection = 0;
    c.struct_size = 4;
    CHECK(moq_media_player_attach(r, &c, &p) == MOQ_ERR_INVAL);
    moq_media_player_cfg_init(&c);
    REQUIRE(moq_media_player_attach(r, &c, &p) == MOQ_OK);
    moq_media_player_output_t small;
    CHECK(moq_media_player_poll_output(p, &small, sizeof(small) - 1) == MOQ_ERR_ABI_MISMATCH);
    moq_media_player_event_t ev;
    CHECK(moq_media_player_poll_event(p, &ev, sizeof(ev) - 1) == MOQ_ERR_ABI_MISMATCH);
    moq_media_player_stats_t st;
    CHECK(moq_media_player_get_stats(p, &st, MOQ_MEDIA_PLAYER_STATS_V0_SIZE - 1) ==
          MOQ_ERR_ABI_MISMATCH);
    CHECK(moq_media_player_receiver(p) == r);
    moq_media_player_destroy(p);
    p = NULL;
    moq_media_player_output_cleanup(NULL);
    moq_media_receiver_test_free(r);

    rcfg.auto_subscribe = true;
    r = moq_media_receiver_test_new_cfg(&rcfg);
    REQUIRE(r != NULL);
    CHECK(moq_media_player_attach(r, &c, &p) == MOQ_ERR_WRONG_STATE);
    moq_media_receiver_test_free(r);
    rcfg.auto_subscribe = false;
    rcfg.surface_stream_terminals = false;
    r = moq_media_receiver_test_new_cfg(&rcfg);
    REQUIRE(r != NULL);
    CHECK(moq_media_player_attach(r, &c, &p) == MOQ_ERR_WRONG_STATE);
    moq_media_receiver_test_free(r);
    rcfg.surface_stream_terminals = true;
    rcfg.time_mode = MOQ_MEDIA_TIME_SHARED_EPOCH;
    r = moq_media_receiver_test_new_cfg(&rcfg);
    REQUIRE(r != NULL);
    CHECK(moq_media_player_attach(r, &c, &p) == MOQ_ERR_UNSUPPORTED);
    moq_media_receiver_test_free(r);
    CHECK(p == NULL);
out:
    if (failures == before) printf("PASS: %s\n", row);
}

/* A polled output stays valid after the player is destroyed, and its
 * cleanup returns its refs then. */
static void test_cleanup_after_destroy(void)
{
    row = "cleanup_after_destroy";
    unsigned before = failures;
    pf_t f;
    REQUIRE(pf_setup(&f, MOQ_VERSION_DRAFT_16, k_cat_v, k_v, 1, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 64);
    cycles(&f, 8);
    moq_media_player_output_t keep;
    memset(&keep, 0, sizeof(keep));
    moq_media_player_output_t o;
    while (moq_media_player_poll_output(f.p, &o, sizeof(o)) == MOQ_OK) {
        if (o.kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT && !keep.kind) keep = o;
        else moq_media_player_output_cleanup(&o);
    }
    REQUIRE(keep.kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT && keep.u.unit.payload);
    moq_media_player_destroy(f.p);
    f.p = NULL;
    CHECK(moq_rcbuf_len(keep.u.unit.payload) == 64);
    moq_media_player_output_cleanup(&keep);
    CHECK(keep.u.unit.payload == NULL);
    moq_media_player_output_cleanup(&keep);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}


static void write_catalog(pf_t *f, uint64_t g, const char *json);

/* The first normal done re-arms the subscription for VOD; the peer refuses
 * the new SUBSCRIBE (the track is gone), and the receiver ends the track
 * with no done status. No END_OF_TRACK can follow: END{PEER_DONE,
 * TAIL_CUTOFF} once the tail wait passes. */
static void test_rearm_refused(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "rearm_refused_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    end_group(&f, "video");
    cycles(&f, 8);
    CHECK(moq_pub_finish_subscribers(f.pub, pf_pub(&f, "video"), 0x2u, f.now) == MOQ_OK);
    CHECK(moq_pub_remove_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
    REQUIRE(until_receiver_ended(&f, "video"));
    expect_bounded_peer_done(&f);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* The control: the re-armed subscription is accepted and stays live, so
 * the track has not ended and nothing ends it, however long. */
static void test_rearm_live(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "rearm_live_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    end_group(&f, "video");
    cycles(&f, 8);
    CHECK(moq_pub_finish_subscribers(f.pub, pf_pub(&f, "video"), 0x2u, f.now) == MOQ_OK);
    out_log_t log = {0};
    for (unsigned i = 0; i < 4 * (TAIL_US / 1000); i++) {
        (void)cycle(&f);
        outputs(&f, &log);
    }
    moq_media_track_state_t state;
    CHECK(moq_media_receiver_track_state(f.r, pf_track(&f, "video"), &state) == MOQ_OK);
    CHECK(state != MOQ_MEDIA_TRACK_STATE_ENDED);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, NULL) == 0);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A terminal normal done and END_OF_TRACK, in either order, with every unit
 * present: END{PEER_END_OF_TRACK, COMPLETE}. With the done first, the
 * END_OF_TRACK arrives within the tail wait (DONE may precede late
 * objects). */
static void test_terminal_done_and_end(moq_version_t ver, bool done_first)
{
    static char label[80];
    snprintf(label, sizeof(label), "terminal_done_and_end_%s_%s",
             done_first ? "done_first" : "end_first", vname(ver));
    row = label;
    unsigned before = failures;
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    write_loc(&f, "video", 0, 1, false, 0, 0);
    end_group(&f, "video");
    cycles(&f, 8);
    if (done_first) {
        terminal_normal_done(&f, ver, "video");
        REQUIRE(until_receiver_ended(&f, "video"));
        cycles(&f, (unsigned)(TAIL_US / 1000) / 2);
        REQUIRE(moq_media_receiver_test_end_of_track(f.r, pf_track(&f, "video"), 0, 2));
    } else {
        CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
        cycles(&f, 4);
        terminal_normal_done(&f, ver, "video");
    }
    out_log_t log = {0};
    for (unsigned i = 0; i < 3 * (TAIL_US / 1000); i++) {
        (void)cycle(&f);
        outputs(&f, &log);
    }
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, NULL) == 1);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_COMPLETE);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, NULL) == 2);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* END_OF_TRACK with a unit missing for good, then a terminal normal done:
 * still END_OF_TRACK's end, TAIL_CUTOFF -- the done does not replace its
 * cause. */
static void test_end_gap_then_done(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "end_gap_then_done_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    write_loc(&f, "video", 0, 2, false, 0, 0);
    CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
    cycles(&f, 4);
    terminal_normal_done(&f, ver, "video");
    out_log_t log = {0};
    for (unsigned i = 0; i < 4 * (TAIL_US / 1000); i++) {
        (void)cycle(&f);
        outputs(&f, &log);
    }
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, NULL) == 1);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A cause during a terminal normal done's wait takes over: the track
 * leaving the catalog ends it REMOVED, with its own tail wait, and no
 * PEER_DONE end is reported. */
static void test_done_then_removed(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "done_then_removed_%s", vname(ver));
    row = label;
    unsigned before = failures;
    static const char cat[] = CATALOG(LOC_VIDEO("video", "") "," LOC_VIDEO("other", ""));
    static const char cat2[] = CATALOG(LOC_VIDEO("other", ""));
    static const char *const names[] = { "video", "other" };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, cat, names, 2, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    end_group(&f, "video");
    cycles(&f, 8);
    terminal_normal_done(&f, ver, "video");
    REQUIRE(until_receiver_ended(&f, "video"));
    write_catalog(&f, 1, cat2);
    out_log_t log = {0};
    for (unsigned i = 0; i < 4 * (TAIL_US / 1000); i++) {
        (void)cycle(&f);
        outputs(&f, &log);
    }
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, NULL) == 1);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_REMOVED);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A normal done is not a cause: with END_OF_TRACK after it, or before it,
 * the end is COMPLETE. */
static void test_done_and_end(moq_version_t ver, bool done_first)
{
    static char label[64];
    snprintf(label, sizeof(label), "done_and_end_%s_%s",
             done_first ? "done_first" : "end_first", vname(ver));
    row = label;
    unsigned before = failures;
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    write_loc(&f, "video", 0, 1, false, 0, 0);
    end_group(&f, "video");
    cycles(&f, 8);
    if (done_first) {
        CHECK(moq_pub_finish_subscribers(f.pub, pf_pub(&f, "video"), 0x2u, f.now) == MOQ_OK);
        cycles(&f, 24);
        CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
    } else {
        CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
        cycles(&f, 4);
        CHECK(moq_pub_finish_subscribers(f.pub, pf_pub(&f, "video"), 0x2u, f.now) == MOQ_OK);
    }
    cycles(&f, 24);
    out_log_t log = {0};
    outputs(&f, &log);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_COMPLETE);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, NULL) >= 2);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

static void write_catalog(pf_t *f, uint64_t g, const char *json)
{
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)json, strlen(json), &pay);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.payload = pay;
    oc.end_of_group = true;
    CHECK(moq_pub_write_object_ex(f->pub, f->catalog, &oc, f->now) == MOQ_OK);
    moq_rcbuf_decref(pay);
    CHECK(moq_pub_end_group(f->pub, f->catalog, f->now) == MOQ_OK);
}

/* A completed broadcast (MSF 11.3): the track's END_OF_TRACK, and the
 * terminal catalog (isComplete, no tracks) removing it, in either order.
 * The removal of a track the peer already ended is no cause: every unit
 * played, the end is END{PEER_END_OF_TRACK, COMPLETE}, and TRACK_REMOVED
 * is still reported once. `gap`: object 0/1 never sent, so the end is
 * not COMPLETE -- coverage, not the removal, decides it. */
static void test_complete_broadcast(moq_version_t ver, bool catalog_first, bool gap)
{
    static char label[80];
    snprintf(label, sizeof(label), "complete_broadcast_%s%s_%s",
             catalog_first ? "catalog_first" : "end_first", gap ? "_gap" : "",
             vname(ver));
    row = label;
    unsigned before = failures;
    static const char terminal[] = "{\"version\":1,\"isComplete\":true,\"tracks\":[]}";
    pf_opts_t o = { .gap_timeout_us = gap ? 200000 : 0 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, gap ? &o : NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    if (!gap) write_loc(&f, "video", 0, 1, false, 0, 0);
    write_loc(&f, "video", 0, 2, false, 0, 0);
    end_group(&f, "video");
    cycles(&f, 8);
    /* The track ends as a completing media sender ends it: END_OF_TRACK,
     * then its removal, which completes the subscription with TRACK_ENDED
     * and the exact Stream Count. */
    if (catalog_first) {
        write_catalog(&f, 1, terminal);
        cycles(&f, 8);
        CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
        CHECK(moq_pub_remove_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
    } else {
        /* The end, and the terminal catalog right behind it, as a publisher
         * completing the broadcast sends them: the removal reaches the
         * player while it is still settling the END_OF_TRACK. */
        CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
        CHECK(moq_pub_remove_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
        write_catalog(&f, 1, terminal);
    }
    ev_log_t evl = {0};
    out_log_t log = {0};
    for (unsigned i = 0; i < 8 * (TAIL_US / 1000); i++) {
        (void)cycle(&f);
        events(&f, &evl);
        outputs(&f, &log);
    }
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_TRACK_REMOVED) == 1);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, NULL) == 1);
    if (gap) CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, NULL) >= 1);
    else     CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, NULL) == 3);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
    CHECK(e->u.end.disposition ==
          (gap ? MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF : MOQ_MEDIA_PLAYER_END_COMPLETE));
    if (failures != before && e)
        fprintf(stderr, "  %s: END cause=%u disposition=%u\n", row,
                (unsigned)e->u.end.cause, (unsigned)e->u.end.disposition);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* One step of the network and the receiver, with no player tick: the
 * receiver's events queue for the player's next tick. */
static void net_step(pf_t *f)
{
    f->now += 1000;
    CHECK(moq_simpair_advance_to(f->sp, f->now) == MOQ_OK);
    CHECK(moq_simpair_run_until_quiescent(f->sp, 64, NULL) >= 0);
    if (f->pub) CHECK(moq_pub_tick(f->pub, f->now) >= 0);
    CHECK(moq_session_process_pending(f->rx, f->now) >= 0);
    moq_media_receiver_test_pump(f->r, f->rx, f->now);
}

/* A withdrawal the player has not yet read, then a terminal catalog: the
 * peer ended video with a normal done, a catalog update withdrew it, and
 * the broadcast then completed -- all taken in by the receiver while the
 * player was not ticked. The withdrawal is still a cause when the player
 * reads it: END{REMOVED}, as if no terminal catalog had followed. */
static void test_withdrawal_then_complete(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "withdrawal_then_complete_%s", vname(ver));
    row = label;
    unsigned before = failures;
    static const char cat[] = CATALOG(LOC_VIDEO("video", "") "," LOC_VIDEO("other", ""));
    static const char cat2[] = CATALOG(LOC_VIDEO("other", ""));
    static const char terminal[] = "{\"version\":1,\"isComplete\":true,\"tracks\":[]}";
    static const char *const names[] = { "video", "other" };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, cat, names, 2, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    end_group(&f, "video");
    cycles(&f, 8);
    /* From here on the player is not ticked until all three are in. */
    if (ver == MOQ_VERSION_DRAFT_22) {
        CHECK(moq_pub_finish_subscribers(f.pub, pf_pub(&f, "video"), 0x2u, f.now) == MOQ_OK);
        for (unsigned i = 0; i < 24; i++) net_step(&f);
        CHECK(moq_pub_finish_subscribers(f.pub, pf_pub(&f, "video"), 0x2u, f.now) == MOQ_OK);
    } else {
        CHECK(moq_pub_finish_subscribers(f.pub, pf_pub(&f, "video"), 0x3u, f.now) == MOQ_OK);
    }
    bool ended = false;
    for (unsigned i = 0; i < 64 && !ended; i++) {
        net_step(&f);
        moq_media_track_state_t state;
        ended = moq_media_receiver_track_state(f.r, pf_track(&f, "video"), &state) ==
                    MOQ_OK && state == MOQ_MEDIA_TRACK_STATE_ENDED;
    }
    REQUIRE(ended);
    write_catalog(&f, 1, cat2);
    for (unsigned i = 0; i < 16; i++) net_step(&f);
    write_catalog(&f, 2, terminal);
    for (unsigned i = 0; i < 16; i++) net_step(&f);
    {   /* Non-vacuity: the receiver has adopted the terminal catalog. */
        moq_media_receiver_stats_t st;
        memset(&st, 0, sizeof(st));
        CHECK(moq_media_receiver_get_stats(f.r, &st, sizeof(st)) == MOQ_OK);
        CHECK(st.catalog_complete);
    }
    ev_log_t evl = {0};
    out_log_t log = {0};
    for (unsigned i = 0; i < 4 * (TAIL_US / 1000); i++) {
        (void)cycle(&f);
        events(&f, &evl);
        outputs(&f, &log);
    }
    /* video by the withdrawal, other by the terminal catalog. */
    size_t removed_video = 0;
    for (size_t i = 0; i < evl.n; i++)
        removed_video += evl.ev[i].kind == MOQ_MEDIA_PLAYER_EVENT_TRACK_REMOVED &&
                         evl.ev[i].track == pf_track(&f, "video");
    CHECK(removed_video == 1);
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_TRACK_REMOVED) == 2);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, NULL) == 1);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->track == pf_track(&f, "video"));
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_REMOVED);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF);
    if (failures != before && e)
        fprintf(stderr, "  %s: END cause=%u disposition=%u\n", row,
                (unsigned)e->u.end.cause, (unsigned)e->u.end.disposition);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A catalog update removes the selected track: TRACK_REMOVED, then after
 * the tail wait END{REMOVED, TAIL_CUTOFF}. */
static void test_removed(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "removed_%s", vname(ver));
    row = label;
    unsigned before = failures;
    static const char cat[] = CATALOG(LOC_VIDEO("video", "") "," LOC_VIDEO("other", ""));
    static const char cat2[] = CATALOG(LOC_VIDEO("other", ""));
    static const char *const names[] = { "video", "other" };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, cat, names, 2, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    cycles(&f, 8);
    write_catalog(&f, 1, cat2);
    ev_log_t evl = {0};
    out_log_t log = {0};
    for (unsigned i = 0; i < 120; i++) {
        (void)cycle(&f);
        events(&f, &evl);
        outputs(&f, &log);
    }
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_TRACK_REMOVED) == 1);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_REMOVED);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* Opus audio starts at any unit and crosses a hole without a reset,
 * reporting it. */
static void test_audio_gap(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "audio_gap_%s", vname(ver));
    row = label;
    unsigned before = failures;
    static const char cat[] = CATALOG(LOC_AUDIO("audio", "opus", ""));
    static const char *const names[] = { "audio" };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, cat, names, 1, NULL));
    REQUIRE(select_plain(&f, "audio") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "audio", 0, 1, false, 0, 0);
    write_loc(&f, "audio", 0, 3, false, 0, 0);
    ev_log_t evl = {0};
    out_log_t log = {0};
    for (unsigned i = 0; i < 40; i++) {
        (void)cycle(&f);
        events(&f, &evl);
        outputs(&f, &log);
    }
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, NULL) == 2);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_DISCONTINUITY, NULL) == 0);
    size_t crossed = 0;
    for (size_t i = 0; i < evl.n; i++)
        crossed += evl.ev[i].kind == MOQ_MEDIA_PLAYER_EVENT_GAP && evl.ev[i].u.gap.crossed;
    CHECK(crossed == 1);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* Draft 22 AAC without a catalog configuration: units before an in-band
 * configuration are dropped CONFIGURATION_MISSING, none is output. */
static void test_aac_config_missing(void)
{
    row = "aac_config_missing";
    unsigned before = failures;
    static const char cat[] = CATALOG(LOC_AUDIO("aac", "mp4a.40.2", ""));
    static const char *const names[] = { "aac" };
    pf_t f;
    REQUIRE(pf_setup(&f, MOQ_VERSION_DRAFT_22, cat, names, 1, NULL));
    REQUIRE(select_plain(&f, "aac") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "aac", 0, 0, false, 0, 0);
    write_loc(&f, "aac", 0, 1, false, 0, 0);
    ev_log_t evl = {0};
    out_log_t log = {0};
    for (unsigned i = 0; i < 16; i++) {
        (void)cycle(&f);
        events(&f, &evl);
        outputs(&f, &log);
    }
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, NULL) == 0);
    size_t missing = 0;
    for (size_t i = 0; i < evl.n; i++)
        missing += evl.ev[i].kind == MOQ_MEDIA_PLAYER_EVENT_DROP &&
                   evl.ev[i].u.drop.reason == MOQ_MEDIA_PLAYER_DROP_CONFIGURATION_MISSING;
    CHECK(missing == 2);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* Deselecting stops the track: its later objects are discarded, no END. */
static void test_deselect(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "deselect_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    cycles(&f, 8);
    out_log_t log = {0};
    outputs(&f, &log);
    moq_media_track_t *v = pf_track(&f, "video");
    REQUIRE(v != NULL);
    CHECK(moq_media_player_deselect_track(f.p, v) == MOQ_OK);
    CHECK(moq_media_player_deselect_track(f.p, v) == MOQ_ERR_INVAL);
    write_loc(&f, "video", 0, 1, false, 0, 0);
    cycles(&f, 16);
    out_log_t after = {0};
    outputs(&f, &after);
    CHECK(count_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, NULL) == 0);
    CHECK(count_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_END, NULL) == 0);
    moq_media_player_stats_t st;
    CHECK(moq_media_player_get_stats(f.p, &st, sizeof(st)) == MOQ_OK);
    CHECK(st.tracks_selected == 0);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A full event queue drops its oldest diagnostics, never ENDED or CLOSED,
 * and never stops playback. */
static void test_event_overflow(void)
{
    row = "event_overflow";
    unsigned before = failures;
    pf_opts_t o = { .max_events = 1, .selection = MOQ_MEDIA_SELECTION_AUTO };
    pf_t f;
    REQUIRE(pf_setup(&f, MOQ_VERSION_DRAFT_16, k_cat_v, k_v, 1, &o));
    cycles(&f, 16);
    events(&f, NULL);
    /* Deltas before an entry: each is dropped with an event, unpolled. */
    for (uint64_t g = 0; g < 8; g++) {
        write_loc(&f, "video", g, 1, false, 0, 0);
        end_group(&f, "video");
        cycles(&f, 2);
    }
    write_loc(&f, "video", 8, 0, true, 0, 0);
    cycles(&f, 16);
    CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
    cycles(&f, 40);
    moq_endpoint_test_set_stopped(f.ep);
    cycles(&f, 8);
    out_log_t log = {0};
    outputs(&f, &log);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, NULL) == 1);
    ev_log_t evl = {0};
    events(&f, &evl);
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_ENDED) == 1);
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_CLOSED) == 1);
    moq_media_player_stats_t st;
    CHECK(moq_media_player_get_stats(f.p, &st, sizeof(st)) == MOQ_OK);
    CHECK(st.events_dropped > 0);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* Removal during END_OF_TRACK's tail wait outranks it and ends the wait:
 * END{REMOVED, TAIL_CUTOFF} comes at once, not at the tail deadline. (A
 * done after END_OF_TRACK is never reported: the receiver ends a track
 * once.) */
static void test_cause_precedence(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "cause_precedence_%s", vname(ver));
    row = label;
    unsigned before = failures;
    static const char cat[] = CATALOG(LOC_VIDEO("video", "") "," LOC_VIDEO("other", ""));
    static const char cat2[] = CATALOG(LOC_VIDEO("other", ""));
    static const char *const names[] = { "video", "other" };
    pf_opts_t o = { .gap_timeout_us = 10000000 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, cat, names, 2, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    write_loc(&f, "video", 0, 2, false, 0, 0);
    CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
    cycles(&f, 4);
    out_log_t log = {0};
    outputs(&f, &log);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, NULL) == 0);
    uint64_t deadline = moq_media_player_next_deadline_us(f.p);
    CHECK(deadline != UINT64_MAX);
    write_catalog(&f, 1, cat2);
    for (unsigned i = 0; i < 12; i++) (void)cycle(&f);
    CHECK(f.now < deadline);
    outputs(&f, &log);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_REMOVED);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A gap settled mid-stream says nothing about the end: a later
 * END_OF_TRACK with everything after it present is COMPLETE. */
static void test_gap_then_complete(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "gap_then_complete_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    write_loc(&f, "video", 0, 2, false, 0, 0);
    end_group(&f, "video");
    write_loc(&f, "video", 1, 0, true, 0, 0);
    end_group(&f, "video");
    for (unsigned i = 0; i < 30; i++) (void)cycle(&f);
    ev_log_t evl = {0};
    events(&f, &evl);
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_GAP) >= 1);
    CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
    cycles(&f, 16);
    out_log_t log = {0};
    outputs(&f, &log);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_COMPLETE);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* Draft 22: a track whose catalog carries no Timescale is wall-clock; an
 * object in media time is dropped TIME_DOMAIN, never a unit. */
static void test_time_domain_drop(void)
{
    row = "time_domain_drop";
    unsigned before = failures;
    pf_t f;
    REQUIRE(pf_setup(&f, MOQ_VERSION_DRAFT_22, k_cat_v, k_v, 1, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 90000, 0);
    write_loc(&f, "video", 0, 1, false, 0, 0);
    cycles(&f, 16);
    ev_log_t evl = {0};
    out_log_t log = {0};
    events(&f, &evl);
    outputs(&f, &log);
    size_t dropped = 0;
    for (size_t i = 0; i < evl.n; i++)
        dropped += evl.ev[i].kind == MOQ_MEDIA_PLAYER_EVENT_DROP &&
                   evl.ev[i].u.drop.reason == MOQ_MEDIA_PLAYER_DROP_TIME_DOMAIN &&
                   evl.ev[i].u.drop.object_id == 0;
    CHECK(dropped == 1);
    for (size_t i = 0; i < log.n; i++)
        if (log.o[i].kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT)
            CHECK(log.o[i].u.unit.object_id != 0);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* The receiver closes with more queued than the engine can hold: every
 * queued object still comes out, then END{CLOSED, IMMEDIATE}. */
static void test_closed_with_backlog(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "closed_with_backlog_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .max_buffered_objects = 2 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    for (uint64_t ob = 0; ob < 8; ob++) write_loc(&f, "video", 0, ob, ob == 0, 0, 0);
    (void)cycle(&f);
    moq_endpoint_test_set_stopped(f.ep);
    size_t units = 0;
    out_log_t log = {0};
    for (unsigned i = 0; i < 40; i++) {
        (void)cycle(&f);
        size_t before_n = log.n;
        outputs(&f, &log);
        for (size_t k = before_n; k < log.n; k++)
            units += log.o[k].kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT;
    }
    CHECK(units == 8);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_CLOSED);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_IMMEDIATE);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

static void pause_decoder(pf_t *f, moq_media_track_t *v, uint64_t gen, uint32_t depth)
{
    moq_media_player_feedback_t fb;
    moq_media_player_feedback_init(&fb);
    fb.kind = MOQ_MEDIA_PLAYER_FEEDBACK_DECODER_QUEUE;
    fb.track = v;
    fb.configuration_generation = gen;
    fb.u.decoder_queue.depth = depth;
    fb.u.decoder_queue.max_recommended = 4;
    CHECK(moq_media_player_handle_feedback(f->p, &fb, f->now) == MOQ_OK);
}

/* An object held for the engine is settled before any later stream fact:
 * while the decoder is paused and the engine full, the last object is held
 * and the END_OF_TRACK behind it, pollable from the receiver, starts no
 * tail wait and ends nothing; once released, every unit comes out and the
 * end is COMPLETE. */
static void test_held_before_facts(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "held_before_facts_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .max_buffered_objects = 2 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc_eog(&f, "video", 0, 0, true, 0, 0, true);
    cycles(&f, 8);
    out_log_t log = {0};
    outputs(&f, &log);
    moq_media_track_t *v = pf_track(&f, "video");
    REQUIRE(v != NULL);
    uint64_t gen = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT)->configuration_generation;
    pause_decoder(&f, v, gen, 9);
    for (uint64_t ob = 1; ob < 4; ob++) write_loc_eog(&f, "video", 0, ob, false, 0, 0, true);
    end_group(&f, "video");
    CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
    cycles(&f, 16);
    moq_media_player_stats_t st;
    CHECK(moq_media_player_get_stats(f.p, &st, sizeof(st)) == MOQ_OK);
    CHECK(st.held_objects == 1);
    CHECK(moq_media_player_next_deadline_us(f.p) == UINT64_MAX);
    pause_decoder(&f, v, gen, 0);
    out_log_t after = {0};
    for (unsigned i = 0; i < 24; i++) {
        (void)cycle(&f);
        outputs(&f, &after);
    }
    CHECK(count_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, v) == 3);
    const moq_media_player_output_t *e = last_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_COMPLETE);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A terminal normal done's wait does not count while the player holds
 * input: with the decoder paused for longer than the tail wait, the object
 * held and the END_OF_TRACK behind it, nothing ends and no deadline runs;
 * once released, every unit comes out and the end is COMPLETE. */
static void test_held_done_waits(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "held_done_waits_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .max_buffered_objects = 2 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc_eog(&f, "video", 0, 0, true, 0, 0, true);
    cycles(&f, 8);
    out_log_t log = {0};
    outputs(&f, &log);
    moq_media_track_t *v = pf_track(&f, "video");
    REQUIRE(v != NULL);
    uint64_t gen = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT)->configuration_generation;
    pause_decoder(&f, v, gen, 9);
    for (uint64_t ob = 1; ob < 4; ob++) write_loc_eog(&f, "video", 0, ob, false, 0, 0, true);
    end_group(&f, "video");
    CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
    cycles(&f, 4);
    terminal_normal_done(&f, ver, "video");
    REQUIRE(until_receiver_ended(&f, "video"));
    out_log_t held = {0};
    for (unsigned i = 0; i < 3 * (TAIL_US / 1000); i++) {
        (void)cycle(&f);
        outputs(&f, &held);
    }
    moq_media_player_stats_t st;
    CHECK(moq_media_player_get_stats(f.p, &st, sizeof(st)) == MOQ_OK);
    CHECK(st.held_objects == 1);
    CHECK(count_out(&held, MOQ_MEDIA_PLAYER_OUTPUT_END, NULL) == 0);
    CHECK(moq_media_player_next_deadline_us(f.p) == UINT64_MAX);
    pause_decoder(&f, v, gen, 0);
    out_log_t after = {0};
    for (unsigned i = 0; i < 24; i++) {
        (void)cycle(&f);
        outputs(&f, &after);
    }
    CHECK(count_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, v) == 3);
    const moq_media_player_output_t *e = last_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_COMPLETE);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* An object larger than a held object may be: taken when the engine has
 * room for it at once, dropped OVERSIZE when it would have to be held. */
static void test_larger_than_held(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "larger_than_held_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .max_buffered_objects = 2, .max_held_bytes = 1000 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 3000);
    cycles(&f, 8);
    out_log_t log = {0};
    outputs(&f, &log);
    moq_media_track_t *v = pf_track(&f, "video");
    REQUIRE(v != NULL);
    const moq_media_player_output_t *u = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT);
    REQUIRE(u != NULL);
    CHECK(u->u.unit.len == 3000);
    pause_decoder(&f, v, u->configuration_generation, 9);
    write_loc(&f, "video", 0, 1, false, 0, 0);
    write_loc(&f, "video", 0, 2, false, 0, 0);
    write_loc(&f, "video", 0, 3, false, 0, 3000);
    cycles(&f, 16);
    ev_log_t evl = {0};
    events(&f, &evl);
    size_t oversize = 0;
    for (size_t i = 0; i < evl.n; i++)
        oversize += evl.ev[i].kind == MOQ_MEDIA_PLAYER_EVENT_DROP &&
                    evl.ev[i].u.drop.reason == MOQ_MEDIA_PLAYER_DROP_OVERSIZE &&
                    evl.ev[i].u.drop.object_id == 3;
    CHECK(oversize == 1);
    moq_media_player_stats_t st;
    CHECK(moq_media_player_get_stats(f.p, &st, sizeof(st)) == MOQ_OK);
    CHECK(st.held_objects == 0);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* -- Bounds, settlement and terminal rows ---------------------------------- */

static uint64_t mono_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

static moq_media_player_stats_t pstats(pf_t *f)
{
    moq_media_player_stats_t st;
    memset(&st, 0, sizeof(st));
    CHECK(moq_media_player_get_stats(f->p, &st, sizeof(st)) == MOQ_OK);
    return st;
}

static moq_media_receiver_t *bare_receiver(void)
{
    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_sized(&rcfg, sizeof(rcfg));
    static moq_bytes_t parts[] = { { (const uint8_t *)"x", 1 } };
    rcfg.namespace_ = (moq_namespace_t){ parts, 1 };
    rcfg.overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
    rcfg.auto_subscribe = false;
    rcfg.surface_stream_terminals = true;
    return moq_media_receiver_test_new_cfg(&rcfg);
}

/* Queue depths are bounded before anything is allocated: a depth past
 * MOQ_MEDIA_PLAYER_MAX_QUEUE, including the values whose terminal reserve
 * would wrap, is refused; the bound itself and the smallest depth attach. */
static void test_queue_bounds(void)
{
    row = "queue_bounds";
    unsigned before = failures;
    static const uint32_t bad[] = { UINT32_MAX, UINT32_MAX - 1, UINT32_MAX - 2,
                                    MOQ_MEDIA_PLAYER_MAX_QUEUE + 1 };
    for (int field = 0; field < 2; field++) {
        for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
            moq_media_receiver_t *r = bare_receiver();
            REQUIRE(r != NULL);
            moq_media_player_cfg_t c;
            moq_media_player_cfg_init(&c);
            if (field == 0) c.max_events = bad[i];
            else c.max_outputs = bad[i];
            moq_media_player_t *p = NULL;
            CHECK(moq_media_player_attach(r, &c, &p) == MOQ_ERR_INVAL);
            CHECK(p == NULL);
            moq_media_receiver_test_free(r);
        }
        static const uint32_t good[] = { MOQ_MEDIA_PLAYER_MAX_QUEUE, 1 };
        for (size_t i = 0; i < 2; i++) {
            moq_media_receiver_t *r = bare_receiver();
            REQUIRE(r != NULL);
            moq_media_player_cfg_t c;
            moq_media_player_cfg_init(&c);
            if (field == 0) c.max_events = good[i];
            else c.max_outputs = good[i];
            moq_media_player_t *p = NULL;
            CHECK(moq_media_player_attach(r, &c, &p) == MOQ_OK);
            moq_media_player_destroy(p);
            moq_media_receiver_test_free(r);
        }
    }
out:
    if (failures == before) printf("PASS: %s\n", row);
}

/* A LOC object carrying a draft-22 in-band Video Config. */
static void write_loc_cfg(pf_t *f, const char *name, uint64_t g, uint64_t o,
                          bool independent, size_t size, const uint8_t *cfg, size_t cfg_len)
{
    moq_loc_metadata_t md;
    moq_loc_metadata_init_sized(&md, sizeof(md));
    md.present = MOQ_LOC_METADATA_HAS_TIMESTAMP | MOQ_LOC_METADATA_HAS_MARKING |
                 MOQ_LOC_METADATA_HAS_VIDEO_CONFIG;
    md.timestamp = ts_of(g, o);
    md.marking_len = 1;
    md.marking[0] = independent ? 0xE0 : 0xC0;
    md.video_config = (moq_bytes_t){ cfg, cfg_len };
    moq_rcbuf_t *props = NULL;
    CHECK(moq_loc_encode_metadata(moq_alloc_default(), f->ver, MOQ_LOC_PROFILE_04, &md,
                                  &props) == MOQ_OK);
    uint8_t big[4096];
    memset(big, (int)(g * 16 + o), sizeof(big));
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(moq_alloc_default(), big, size, &pay);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.object_id = o;
    oc.payload = pay;
    oc.properties = props;
    CHECK(moq_pub_write_object_ex(f->pub, pf_pub(f, name), &oc, f->now) == MOQ_OK);
    moq_rcbuf_decref(pay);
    moq_rcbuf_decref(props);
}

/* An output's bytes stay charged to the engine budget until it is polled:
 * with a 9000-byte budget and 4000-byte units, the unpolled outputs never
 * keep more than the budget, the engine refuses the rest (they are held,
 * then queue in the receiver), a slow consumer still gets every unit in
 * order with nothing dropped, and once every output is polled none is
 * charged. On draft 22 a configuration change rides along: a queued
 * CONFIGURE(A) keeps A charged after the track moves on to B. */
static void test_output_charge(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "output_charge_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .max_buffered_bytes = 9000, .max_outputs = 64 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    static const uint8_t cfg_a[600] = { 0x01 }, cfg_b[700] = { 0x02 };
    bool d22 = ver == MOQ_VERSION_DRAFT_22;
    for (uint64_t i = 0; i < 8; i++) {
        if (d22 && i == 0) write_loc_cfg(&f, "video", 0, 0, true, 4000, cfg_a, sizeof(cfg_a));
        else if (d22 && i == 1) write_loc_cfg(&f, "video", 0, 1, false, 2000, cfg_b, sizeof(cfg_b));
        else write_loc(&f, "video", 0, i, i == 0, 0, i == 1 ? 2000 : 4000);
        cycles(&f, 4);
    }
    moq_media_player_stats_t st = pstats(&f);
    CHECK(st.outputs_bytes > 0 && st.outputs_bytes <= 9000);
    if (d22) CHECK(st.outputs_bytes >= sizeof(cfg_a));
    uint64_t next = 0;
    bool held = false;
    for (unsigned i = 0; i < 400 && next < 8; i++) {
        (void)cycle(&f);
        st = pstats(&f);
        CHECK(st.outputs_bytes <= 9000);
        held |= st.held_objects > 0;
        moq_media_player_output_t x;
        if (moq_media_player_poll_output(f.p, &x, sizeof(x)) == MOQ_OK) {
            if (x.kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT) {
                CHECK(x.u.unit.object_id == next);
                next++;
            }
            moq_media_player_output_cleanup(&x);
        }
        events(&f, NULL);
    }
    CHECK(next == 8);
    CHECK(held);
    outputs(&f, NULL);
    CHECK(pstats(&f).outputs_bytes == 0);
    moq_media_receiver_stats_t rs;
    CHECK(moq_media_receiver_get_stats(f.r, &rs, sizeof(rs)) == MOQ_OK);
    CHECK(rs.objects_dropped == 0);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A refusal the engine has no event room to record is held, not lost:
 * the cursor waits on (0, 1); END_OF_GROUP FINs make (1, 1..) and (3, 1..)
 * absent, filling a two-range table; a decode-error report fills the
 * one-slot event queue. (5, 0), in media time on a wall-clock track, is
 * refused TIME_DOMAIN, and recording it would coalesce absence, which
 * needs event room: the refusal is held and (5, 1) waits behind it. Once
 * the queue drains it is recorded (KNOWLEDGE_FORGOTTEN), (5, 1) is taken,
 * and nothing else is dropped. */
static void test_held_refusal(void)
{
    row = "held_refusal";
    unsigned before = failures;
    pf_opts_t o = { .gap_timeout_us = UINT64_MAX, .engine_max_events = 1,
                    .engine_max_known_ranges = 2 };
    pf_t f;
    REQUIRE(pf_setup(&f, MOQ_VERSION_DRAFT_22, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video");
    cycles(&f, 16);
    write_loc_eog(&f, "video", 0, 0, true, 0, 0, false);
    write_loc_eog(&f, "video", 1, 0, true, 0, 0, true);
    end_group(&f, "video");
    write_loc_eog(&f, "video", 3, 0, true, 0, 0, true);
    end_group(&f, "video");
    out_log_t log = {0};
    ev_log_t evl = {0};
    for (unsigned i = 0; i < 12; i++) {
        (void)cycle(&f);
        outputs(&f, &log);
        events(&f, &evl);
    }
    uint64_t gen = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT)->configuration_generation;
    write_loc_eog(&f, "video", 5, 0, true, 90000, 0, false);
    write_loc_eog(&f, "video", 5, 1, false, 0, 0, false);
    for (unsigned i = 0; i < 6; i++) {
        f.now += 1000;
        CHECK(moq_simpair_advance_to(f.sp, f.now) == MOQ_OK);
        CHECK(moq_simpair_run_until_quiescent(f.sp, 64, NULL) >= 0);
        CHECK(moq_pub_tick(f.pub, f.now) >= 0);
        CHECK(moq_session_process_pending(f.rx, f.now) >= 0);
        moq_media_receiver_test_pump(f.r, f.rx, f.now);
    }
    uint64_t taken = pstats(&f).objects_taken;
    moq_media_player_feedback_t fb;
    moq_media_player_feedback_init(&fb);
    fb.kind = MOQ_MEDIA_PLAYER_FEEDBACK_DECODE_ERROR;
    fb.track = v;
    fb.configuration_generation = gen;
    CHECK(moq_media_player_handle_feedback(f.p, &fb, f.now) == MOQ_OK);
    CHECK(moq_media_player_tick(f.p, f.now) >= 0);
    events(&f, &evl);
    for (unsigned i = 0; i < 8; i++) {
        (void)cycle(&f);
        events(&f, &evl);
        outputs(&f, NULL);
    }
    size_t refused = 0;
    for (size_t k = 0; k < evl.n; k++)
        refused += evl.ev[k].kind == MOQ_MEDIA_PLAYER_EVENT_DROP &&
                   evl.ev[k].u.drop.reason == MOQ_MEDIA_PLAYER_DROP_TIME_DOMAIN &&
                   evl.ev[k].u.drop.group_id == 5 && evl.ev[k].u.drop.object_id == 0;
    CHECK(refused == 1);
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_KNOWLEDGE_FORGOTTEN) == 1);
    CHECK(pstats(&f).objects_taken == taken + 2);
    CHECK(pstats(&f).objects_dropped == 1);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A refusal is recorded at any valid location, the largest group too:
 * video a's (0, 1) is buffered with no entry in a one-object engine, and
 * b's independent object, too large to hold (one byte may be), is refused
 * OVERSIZE. Its refusal settles the request it made for room, so a is not
 * evicted for it, at group 0 and at group UINT64_MAX alike (times given
 * explicitly, not derived from the group). */
static void test_refusal_at_any_group(uint64_t group)
{
    static char label[64];
    snprintf(label, sizeof(label), "refusal_at_group_%s", group ? "max" : "0");
    row = label;
    unsigned before = failures;
    static const char *const names[] = { "a", "b" };
    static const char cat[] = CATALOG(LOC_VIDEO("a", "") "," LOC_VIDEO("b", ""));
    pf_opts_t o = { .max_buffered_objects = 1, .max_held_bytes = 1 };
    pf_t f;
    REQUIRE(pf_setup(&f, MOQ_VERSION_DRAFT_18, cat, names, 2, &o));
    REQUIRE(select_plain(&f, "a") == MOQ_OK);
    REQUIRE(select_plain(&f, "b") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "a", 0, 1, false, 0, 4);
    cycles(&f, 4);
    outputs(&f, NULL);
    events(&f, NULL);
    moq_playback_t *pb = moq_media_player_test_engine(f.p);
    REQUIRE(pb && moq_playback_test_buffered_count(pb) == 1);
    moq_rcbuf_t *pay = NULL;
    REQUIRE(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"data", 4, &pay) == MOQ_OK);
    bool injected = moq_media_receiver_test_inject_object(f.r, pf_track(&f, "b"), group, 0,
                                                          true, TS0, pay);
    moq_rcbuf_decref(pay);
    REQUIRE(injected);
    cycles(&f, 4);
    ev_log_t evl = {0};
    events(&f, &evl);
    size_t refused = 0, evicted = 0;
    for (size_t i = 0; i < evl.n; i++) {
        const moq_media_player_event_t *e = &evl.ev[i];
        if (e->kind != MOQ_MEDIA_PLAYER_EVENT_DROP) continue;
        refused += e->track == pf_track(&f, "b") && e->u.drop.group_id == group &&
                   e->u.drop.reason == MOQ_MEDIA_PLAYER_DROP_OVERSIZE;
        evicted += e->track == pf_track(&f, "a") &&
                   e->u.drop.reason == MOQ_PLAYBACK_DROP_KEYFRAME_WAIT;
    }
    CHECK(refused == 1);
    CHECK(evicted == 0);
    CHECK(moq_playback_test_buffered_count(pb) == 1);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A player whose engine can hold no more commands never queues one
 * unheld: with four output slots and four hold identities left to issue
 * (a private issuer, so the library's own is not spent), 4000-byte
 * units and a 9000-byte budget, a slow consumer gets units while holds
 * last; then the track ends LOCAL_FAILURE, IMMEDIATE, the player reports
 * ENDED, and tick is not left blocked. Queued outputs never keep more than
 * the budget, and once all are polled none keeps anything. */
static void test_holds_exhausted(void)
{
    row = "holds_exhausted";
    unsigned before = failures;
    pf_opts_t o = { .max_buffered_bytes = 9000, .max_outputs = 4 };
    pf_t f;
    REQUIRE(pf_setup(&f, MOQ_VERSION_DRAFT_18, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video");
    moq_playback_t *pb = moq_media_player_test_engine(f.p);
    REQUIRE(pb != NULL);
    moq_playback_test_local_issuer(pb, 4);   /* four holds left to issue */
    cycles(&f, 16);
    for (uint64_t i = 0; i < 8; i++) {
        write_loc(&f, "video", 0, i, i == 0, 0, 4000);
        cycles(&f, 2);
    }
    out_log_t log = {0};
    ev_log_t evl = {0};
    for (unsigned i = 0; i < 200; i++) {
        (void)cycle(&f);
        CHECK(pstats(&f).outputs_bytes <= 9000);
        moq_media_player_output_t x;
        if (moq_media_player_poll_output(f.p, &x, sizeof(x)) == MOQ_OK) {
            if (log.n < sizeof(log.o) / sizeof(log.o[0])) log.o[log.n++] = x;
            else moq_media_player_output_cleanup(&x);
        }
        events(&f, &evl);
    }
    size_t units = count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, v);
    CHECK(units > 0 && units < 8);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_IMMEDIATE);
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_ENDED) == 1);
    CHECK(pstats(&f).outputs_bytes == 0);
    CHECK(moq_media_player_tick(f.p, f.now) == MOQ_OK);   /* not blocked for ever */
    for (size_t i = 0; i < log.n; i++) moq_media_player_output_cleanup(&log.o[i]);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* Queued outputs keep what they reference charged, configurations
 * included, in actual bytes: eight draft-22 objects of 400 payload bytes,
 * each with its own 600-byte Video Config, into a 5000-byte budget with no
 * output polled. What the queued outputs reference (each buffer once)
 * stays within the budget and is what outputs_bytes reports; intake holds
 * the rest back. Then all eight units come out in order, none dropped,
 * and nothing stays charged. */
static void test_replaced_configs_bounded(void)
{
    row = "replaced_configs_bounded";
    unsigned before = failures;
    pf_opts_t o = { .max_buffered_bytes = 5000, .max_outputs = 64 };
    pf_t f;
    REQUIRE(pf_setup(&f, MOQ_VERSION_DRAFT_22, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    static uint8_t cfgs[8][600];
    for (uint64_t i = 0; i < 8; i++) {
        memset(cfgs[i], (int)(0x10 + i), sizeof(cfgs[i]));
        write_loc_cfg(&f, "video", 0, i, true, 400, cfgs[i], sizeof(cfgs[i]));
    }
    cycles(&f, 40);
    uint64_t reported = pstats(&f).outputs_bytes;
    moq_media_player_output_t q[64];
    size_t n = 0;
    while (n < 64 && moq_media_player_poll_output(f.p, &q[n], sizeof(q[n])) == MOQ_OK) n++;
    const moq_rcbuf_t *seen[64];
    size_t ns = 0;
    uint64_t actual = 0;
    uint64_t next = 0;
    for (size_t i = 0; i < n; i++) {
        const moq_rcbuf_t *b = q[i].kind == MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE
            ? q[i].u.configure.codec_config
            : q[i].kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT ? q[i].u.unit.payload : NULL;
        if (q[i].kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT) CHECK(q[i].u.unit.object_id == next++);
        if (!b) continue;
        bool dup = false;
        for (size_t k = 0; k < ns; k++) dup = dup || seen[k] == b;
        if (dup) continue;
        seen[ns++] = b;
        actual += moq_rcbuf_len(b);
    }
    CHECK(actual > 0 && actual <= 5000);
    CHECK(reported == actual);
    CHECK(next < 8);                                  /* intake held back */
    for (size_t i = 0; i < n; i++) moq_media_player_output_cleanup(&q[i]);
    for (unsigned i = 0; i < 200 && next < 8; i++) {
        (void)cycle(&f);
        moq_media_player_output_t x;
        while (moq_media_player_poll_output(f.p, &x, sizeof(x)) == MOQ_OK) {
            if (x.kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT) CHECK(x.u.unit.object_id == next++);
            moq_media_player_output_cleanup(&x);
        }
        events(&f, NULL);
    }
    CHECK(next == 8);
    CHECK(pstats(&f).objects_dropped == 0);
    CHECK(pstats(&f).outputs_bytes == 0);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* One paused decoder does not hide other work: with video paused by
 * DECODER_QUEUE feedback, an audio object in the receiver is work, so
 * wait says OK and tick plays it. Commands the engine still holds after
 * the application drained a full output queue are work too, with no new
 * receiver traffic. Only when every remaining piece of work waits on
 * feedback is the answer DONE. */
static void test_paused_track_input(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "paused_track_input_%s", vname(ver));
    row = label;
    unsigned before = failures;
    static const char *const names[] = { "video", "audio" };
    static const char cat[] = CATALOG(LOC_VIDEO("video", "") "," LOC_AUDIO("audio", "opus", ""));
    pf_t f;
    REQUIRE(pf_setup(&f, ver, cat, names, 2, NULL));
    moq_endpoint_test_set_waitable(f.ep);
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    REQUIRE(select_plain(&f, "audio") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    write_loc(&f, "audio", 0, 0, true, 0, 0);
    cycles(&f, 8);
    out_log_t log = {0};
    outputs(&f, &log);
    moq_media_track_t *video = pf_track(&f, "video");
    moq_media_track_t *audio = pf_track(&f, "audio");
    REQUIRE(video && audio);
    uint64_t vgen = 0;
    for (size_t i = 0; i < log.n; i++)
        if (log.o[i].track == video) vgen = log.o[i].configuration_generation;
    pause_decoder(&f, video, vgen, 9);
    events(&f, NULL);
    write_loc(&f, "video", 0, 1, false, 0, 0);
    write_loc(&f, "audio", 0, 1, true, 0, 0);
    for (unsigned i = 0; i < 8; i++) {
        f.now += 1000;
        CHECK(moq_simpair_advance_to(f.sp, f.now) == MOQ_OK);
        CHECK(moq_simpair_run_until_quiescent(f.sp, 64, NULL) >= 0);
        CHECK(moq_pub_tick(f.pub, f.now) >= 0);
        CHECK(moq_session_process_pending(f.rx, f.now) >= 0);
        moq_media_receiver_test_pump(f.r, f.rx, f.now);
    }
    CHECK(moq_media_player_wait(f.p, f.now, 0) == MOQ_OK);
    CHECK(moq_media_player_tick(f.p, f.now) >= 0);
    out_log_t after = {0};
    outputs(&f, &after);
    events(&f, NULL);
    CHECK(count_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, audio) == 1);
    CHECK(count_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, video) == 0);
    /* Everything left waits on the video decoder. */
    CHECK(moq_media_player_tick(f.p, f.now) >= 0);
    outputs(&f, NULL);
    events(&f, NULL);
    CHECK(moq_media_player_wait(f.p, f.now, 1000000) == MOQ_DONE);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

static void test_engine_backlog_is_work(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "engine_backlog_is_work_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .max_outputs = 2 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    moq_endpoint_test_set_waitable(f.ep);
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    for (uint64_t ob = 0; ob < 6; ob++) write_loc(&f, "video", 0, ob, ob == 0, 0, 0);
    moq_result_t rc = MOQ_OK;
    for (unsigned i = 0; i < 16 && rc != MOQ_ERR_WOULD_BLOCK; i++) rc = cycle(&f);
    REQUIRE(rc == MOQ_ERR_WOULD_BLOCK);
    size_t units = 0;
    for (unsigned i = 0; i < 16 && units < 6; i++) {
        out_log_t log = {0};
        outputs(&f, &log);
        events(&f, NULL);
        units += count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, NULL);
        if (units >= 6) break;
        /* No receiver traffic: what is left sits in the engine. */
        CHECK(moq_media_player_wait(f.p, f.now, 1000000) == MOQ_OK);
        (void)moq_media_player_tick(f.p, f.now);
    }
    CHECK(units == 6);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* Video waits behind a unit missing for good, its engine full and an
 * object held, when its subscription ends normally for good. Audio's
 * decoder is paused with nothing of audio queued or held: that holds up
 * nothing of video's, so video's wait counts and video ends
 * END{PEER_DONE, TAIL_CUTOFF}, the held object released. The same with
 * audio unpaused (the control). */
static void test_unrelated_pause(moq_version_t ver, bool pause_audio)
{
    static char label[64];
    snprintf(label, sizeof(label), "%s_%s", pause_audio ? "unrelated_pause" : "unpaused_peer",
             vname(ver));
    row = label;
    unsigned before = failures;
    static const char *const names[] = { "video", "audio" };
    static const char cat[] = CATALOG(LOC_VIDEO("video", "") "," LOC_AUDIO("audio", "opus", ""));
    pf_opts_t o = { .gap_timeout_us = UINT64_MAX, .max_buffered_objects = 2 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, cat, names, 2, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    REQUIRE(select_plain(&f, "audio") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    write_loc(&f, "audio", 0, 0, true, 0, 0);
    cycles(&f, 8);
    out_log_t first = {0};
    outputs(&f, &first);
    moq_media_track_t *v = pf_track(&f, "video");
    moq_media_track_t *a = pf_track(&f, "audio");
    uint64_t audio_gen = 0;
    for (size_t i = 0; i < first.n; i++)
        if (first.o[i].kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT && first.o[i].track == a)
            audio_gen = first.o[i].configuration_generation;
    REQUIRE(audio_gen != 0);
    if (pause_audio) pause_decoder(&f, a, audio_gen, 9);
    for (uint64_t ob = 2; ob <= 4; ob++) write_loc(&f, "video", 0, ob, ob == 2, 0, 0);
    cycles(&f, 4);
    REQUIRE(pstats(&f).held_objects == 1);
    terminal_normal_done(&f, ver, "video");
    REQUIRE(until_receiver_ended(&f, "video"));
    out_log_t log = {0};
    for (unsigned i = 0; i < 4 * (TAIL_US / 1000); i++) {
        (void)cycle(&f);
        outputs(&f, &log);
        events(&f, NULL);
    }
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, v) == 1);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    if (e) {
        CHECK(e->track == v);
        CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_DONE);
        CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF);
    }
    CHECK(pstats(&f).held_objects == 0);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A paused decoder holds the wait for as long as it stays paused, however
 * long the caller sleeps: video paused with one object held, its
 * subscription ended normally for good, the wait recorded as held up; the
 * caller's clock jumps three tail waits with no tick, then the decoder
 * resumes. None of the paused interval counts: no END on the first tick,
 * and an END_OF_TRACK right after still ends the track COMPLETE. With
 * `eot` false (the control), the END comes only once the remaining wait
 * has passed from the resume. */
static void test_sparse_pause(moq_version_t ver, bool eot)
{
    static char label[64];
    snprintf(label, sizeof(label), "sparse_pause_%s_%s", eot ? "end" : "cutoff", vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .max_buffered_objects = 2 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc_eog(&f, "video", 0, 0, true, 0, 0, true);
    cycles(&f, 8);
    out_log_t first = {0};
    outputs(&f, &first);
    const moq_media_player_output_t *u = last_out(&first, MOQ_MEDIA_PLAYER_OUTPUT_UNIT);
    REQUIRE(u != NULL);
    moq_media_track_t *v = pf_track(&f, "video");
    uint64_t gen = u->configuration_generation;
    pause_decoder(&f, v, gen, 9);
    for (uint64_t ob = 1; ob <= 3; ob++) write_loc_eog(&f, "video", 0, ob, false, 0, 0, true);
    end_group(&f, "video");
    cycles(&f, 4);
    REQUIRE(pstats(&f).held_objects == 1);
    terminal_normal_done(&f, ver, "video");
    REQUIRE(until_receiver_ended(&f, "video"));
    (void)cycle(&f);
    CHECK(moq_media_player_next_deadline_us(f.p) == UINT64_MAX);
    f.now += 3 * TAIL_US;                    /* the caller sleeps, no tick */
    pause_decoder(&f, v, gen, 0);
    uint64_t resumed = f.now;
    uint64_t deadline = moq_media_player_next_deadline_us(f.p);
    CHECK(deadline > resumed && deadline <= resumed + TAIL_US);
    (void)cycle(&f);
    out_log_t log = {0};
    outputs(&f, &log);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, v) == 0);
    if (eot) {
        REQUIRE(moq_media_receiver_test_end_of_track(f.r, v, 0, 4));
        for (unsigned i = 0; i < TAIL_US / 1000; i++) {
            (void)cycle(&f);
            outputs(&f, &log);
            events(&f, NULL);
        }
        const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
        REQUIRE(e != NULL);
        CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
        CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_COMPLETE);
    } else {
        uint64_t due = moq_media_player_next_deadline_us(f.p);
        CHECK(due == deadline);
        if (due > f.now + 1000) {
            (void)cycle_to(&f, due - 1);
            outputs(&f, &log);
            CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, v) == 0);
        }
        (void)cycle_to(&f, due);
        for (unsigned i = 0; i < 8; i++) {
            (void)cycle(&f);
            outputs(&f, &log);
            events(&f, NULL);
        }
        CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, v) == 1);
        const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
        if (e) {
            CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_DONE);
            CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF);
        }
    }
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* Output the application leaves unpolled, with input held, holds the wait
 * too, and its release by polling -- which carries no clock -- counts only
 * from the next tick: the track's first outputs never polled, (0, 1)
 * missing for good, (0, 2) and (0, 3) in a two-object engine and (0, 4)
 * held, the subscription ended normally for good. The caller sleeps three tail
 * waits, polls everything and ticks: no END on that tick; the END comes
 * once the remaining wait has passed from it. */
static void test_full_output_release(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "full_output_release_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .gap_timeout_us = UINT64_MAX, .max_buffered_objects = 2,
                    .max_outputs = 2 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video");
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    cycles(&f, 8);                           /* outputs left unpolled throughout */
    for (uint64_t ob = 2; ob <= 4; ob++) write_loc(&f, "video", 0, ob, ob == 2, 0, 0);
    cycles(&f, 4);
    REQUIRE(pstats(&f).held_objects == 1);
    terminal_normal_done(&f, ver, "video");
    REQUIRE(until_receiver_ended(&f, "video"));
    (void)cycle(&f);
    REQUIRE(pstats(&f).outputs_queued > 0);
    CHECK(moq_media_player_next_deadline_us(f.p) == UINT64_MAX);
    f.now += 3 * TAIL_US;                    /* the caller sleeps, no tick */
    out_log_t log = {0};
    uint64_t slept = f.now;
    /* Release by polling (no clock), then tick; what the engine had queued
     * behind the full queue comes out and fills it again until it is all
     * polled, so this repeats a few times before the wait counts again. */
    uint64_t due = UINT64_MAX;
    for (unsigned i = 0; i < 8 && due == UINT64_MAX; i++) {
        outputs(&f, &log);
        (void)cycle(&f);
        due = moq_media_player_next_deadline_us(f.p);
    }
    outputs(&f, &log);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, v) == 0);
    /* None of the sleep counted: the remaining wait lies ahead. */
    CHECK(due != UINT64_MAX && due > f.now && due > slept + TAIL_US / 2);
    for (unsigned i = 0; i < 4 * (TAIL_US / 1000); i++) {
        (void)cycle(&f);
        outputs(&f, &log);
        events(&f, NULL);
    }
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, v) == 1);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    if (e) {
        CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_DONE);
        CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF);
    }
    CHECK(pstats(&f).held_objects == 0);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* -- Shared capacity held by the application -------------------------------
 *
 * Video (A) and audio (B) both anchored and started; B's decoder paused
 * with B's media buffered, filling the engine's shared budget (`bytes`: its
 * byte budget; otherwise its object slots). A's next object then waits for
 * admission -- not shed (relief drops only from tracks without an entry),
 * held by the player -- on what only the application can release. */

typedef struct {
    pf_t               f;
    moq_media_track_t *va, *ab;
    uint64_t           audio_gen;
    size_t             asize;
} shared_t;

static bool shared_setup(shared_t *s, moq_version_t ver, bool bytes)
{
    static const char *const names[] = { "video", "audio" };
    static const char cat[] = CATALOG(LOC_VIDEO("video", "") "," LOC_AUDIO("audio", "opus", ""));
    pf_opts_t o = { .gap_timeout_us = UINT64_MAX };
    const size_t bsize = bytes ? 1500 : 8;
    s->asize = bytes ? 1500 : 8;
    if (bytes) {
        o.max_buffered_objects = 16;
        o.max_buffered_bytes = 4000;
    } else {
        o.max_buffered_objects = 2;
    }
    pf_t *f = &s->f;
    if (!pf_setup(f, ver, cat, names, 2, &o)) return false;
    if (select_plain(f, "video") != MOQ_OK || select_plain(f, "audio") != MOQ_OK)
        return false;
    s->va = pf_track(f, "video");
    s->ab = pf_track(f, "audio");
    cycles(f, 16);
    write_loc_eog(f, "video", 0, 0, true, 0, 8, true);
    write_loc(f, "audio", 0, 0, true, 0, 8);
    out_log_t first = {0};
    for (unsigned i = 0; i < 8; i++) {      /* both anchor and start */
        (void)cycle(f);
        outputs(f, &first);
        events(f, NULL);
    }
    s->audio_gen = 0;
    size_t a_units = 0;
    for (size_t i = 0; i < first.n; i++) {
        if (first.o[i].kind != MOQ_MEDIA_PLAYER_OUTPUT_UNIT) continue;
        if (first.o[i].track == s->ab) s->audio_gen = first.o[i].configuration_generation;
        if (first.o[i].track == s->va && first.o[i].u.unit.object_id == 0) a_units++;
    }
    if (s->audio_gen == 0 || a_units != 1) return false;
    pause_decoder(f, s->ab, s->audio_gen, 9);
    write_loc(f, "audio", 0, 1, true, 0, bsize);
    write_loc(f, "audio", 0, 2, true, 0, bsize);
    for (unsigned i = 0; i < 4; i++) {
        (void)cycle(f);
        outputs(f, NULL);
        events(f, NULL);
    }
    return true;
}

/* The hold is genuine: A's object held, B's media buffered, nothing shed
 * or dropped, no END for A. */
static bool shared_hold_genuine(shared_t *s, const ev_log_t *evl, const out_log_t *log)
{
    unsigned before = failures;
    CHECK(pstats(&s->f).held_objects == 1);
    CHECK(moq_playback_test_buffered_count(moq_media_player_test_engine(s->f.p)) >= 2);
    CHECK(count_ev(evl, MOQ_MEDIA_PLAYER_EVENT_DROP) == 0);
    CHECK(count_out(log, MOQ_MEDIA_PLAYER_OUTPUT_END, s->va) == 0);
    return failures == before;
}

/* Exactly these units of `t`, in order, and nothing of it after its END
 * (when `end`). */
static void expect_units(const out_log_t *log, moq_media_track_t *t,
                         const uint64_t (*ids)[2], size_t n, bool end)
{
    size_t k = 0;
    bool ended = false;
    for (size_t i = 0; i < log->n; i++) {
        const moq_media_player_output_t *o = &log->o[i];
        if (o->track != t) continue;
        CHECK(!ended);
        if (o->kind == MOQ_MEDIA_PLAYER_OUTPUT_END) ended = true;
        if (o->kind != MOQ_MEDIA_PLAYER_OUTPUT_UNIT) continue;
        CHECK(k < n);
        if (k < n) {
            CHECK(o->u.unit.group_id == ids[k][0]);
            CHECK(o->u.unit.object_id == ids[k][1]);
        }
        k++;
    }
    CHECK(k == n);
    CHECK(ended == end);
}

static void shared_poll(shared_t *s, unsigned n, out_log_t *log, ev_log_t *evl)
{
    for (unsigned i = 0; i < n; i++) {
        (void)cycle(&s->f);
        outputs(&s->f, log);
        events(&s->f, evl);
    }
}

/* A's done waits behind the hold. A's (0, 1) held, its group and track
 * ended (the END_OF_TRACK behind the held object), A's subscription ended
 * normally for good. The application keeps polling for longer than the
 * tail wait, then resumes B: A's held object goes in, its END_OF_TRACK is
 * taken, and A ends END{PEER_END_OF_TRACK, COMPLETE} after exactly its
 * own units, with B's buffered units released once each -- never a cutoff
 * latched while B was paused. */
static void test_shared_capacity_pause(moq_version_t ver, bool bytes)
{
    static char label[64];
    snprintf(label, sizeof(label), "shared_capacity_pause_%s_%s",
             bytes ? "bytes" : "slots", vname(ver));
    row = label;
    unsigned before = failures;
    shared_t s;
    REQUIRE(shared_setup(&s, ver, bytes));
    pf_t *f = &s.f;
    write_loc_eog(f, "video", 0, 1, false, 0, s.asize, true);
    end_group(f, "video");
    CHECK(moq_pub_end_track(f->pub, pf_pub(f, "video"), f->now) == MOQ_OK);
    cycles(f, 4);
    terminal_normal_done(f, ver, "video");
    REQUIRE(until_receiver_ended(f, "video"));
    ev_log_t evl = {0};
    out_log_t log = {0};
    shared_poll(&s, 3 * (TAIL_US / 1000), &log, &evl);
    REQUIRE(shared_hold_genuine(&s, &evl, &log));
    pause_decoder(f, s.ab, s.audio_gen, 0);
    shared_poll(&s, 2 * (TAIL_US / 1000), &log, &evl);
    static const uint64_t a_ids[][2] = { { 0, 1 } };
    static const uint64_t b_ids[][2] = { { 0, 1 }, { 0, 2 } };
    expect_units(&log, s.va, a_ids, 1, true);
    expect_units(&log, s.ab, b_ids, 2, false);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, s.va) == 1);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL && e->track == s.va);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_COMPLETE);
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_DROP) == 0);
    CHECK(pstats(f).held_objects == 0);
out:
    pf_teardown(&s.f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A's END_OF_TRACK tail waits behind the hold. The END_OF_TRACK (no object
 * at or past (1, 2)) arrives before group 1's delayed stream; its first
 * object is held. The application keeps polling for longer than the tail
 * wait, then resumes B: both of A's late units come out and A ends
 * END{PEER_END_OF_TRACK, COMPLETE}, not a cutoff of a wait the application
 * held up. */
static void test_shared_capacity_eot(moq_version_t ver, bool bytes)
{
    static char label[64];
    snprintf(label, sizeof(label), "shared_capacity_eot_%s_%s",
             bytes ? "bytes" : "slots", vname(ver));
    row = label;
    unsigned before = failures;
    shared_t s;
    REQUIRE(shared_setup(&s, ver, bytes));
    pf_t *f = &s.f;
    end_group(f, "video");
    cycles(f, 2);
    REQUIRE(moq_media_receiver_test_end_of_track(f->r, s.va, 1, 2));
    cycles(f, 2);
    write_loc_eog(f, "video", 1, 0, true, 0, s.asize, true);
    write_loc_eog(f, "video", 1, 1, false, 0, s.asize, true);
    end_group(f, "video");
    ev_log_t evl = {0};
    out_log_t log = {0};
    shared_poll(&s, 3 * (TAIL_US / 1000), &log, &evl);
    REQUIRE(shared_hold_genuine(&s, &evl, &log));
    pause_decoder(f, s.ab, s.audio_gen, 0);
    shared_poll(&s, 2 * (TAIL_US / 1000), &log, &evl);
    static const uint64_t a_ids[][2] = { { 1, 0 }, { 1, 1 } };
    static const uint64_t b_ids[][2] = { { 0, 1 }, { 0, 2 } };
    expect_units(&log, s.va, a_ids, 2, true);
    expect_units(&log, s.ab, b_ids, 2, false);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, s.va) == 1);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL && e->track == s.va);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_COMPLETE);
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_DROP) == 0);
    CHECK(pstats(f).held_objects == 0);
out:
    pf_teardown(&s.f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* The wait is held up for as long as the hold lasts, however long the
 * caller sleeps, and resumes with what was left once it ends. With `eot`,
 * A's END_OF_TRACK tail (as shared_capacity_eot, with (1, 1) never sent);
 * otherwise A's done (as shared_capacity_pause, with no END_OF_TRACK
 * sent). The caller sleeps three tail waits with no tick, then resumes B:
 * the next deadline lies ahead by what was left (none of the sleep
 * counted). The held object goes in; nothing more comes, so the track ends
 * a cutoff exactly at that deadline: no END a millisecond before it, one
 * END{cause, TAIL_CUTOFF} at it, after the held unit. */
static void test_shared_capacity_sparse(moq_version_t ver, bool eot)
{
    static char label[64];
    snprintf(label, sizeof(label), "shared_capacity_sparse_%s_%s",
             eot ? "eot" : "done", vname(ver));
    row = label;
    unsigned before = failures;
    shared_t s;
    REQUIRE(shared_setup(&s, ver, false));
    pf_t *f = &s.f;
    if (eot) {
        end_group(f, "video");
        cycles(f, 2);
        REQUIRE(moq_media_receiver_test_end_of_track(f->r, s.va, 1, 2));
        cycles(f, 2);
        write_loc_eog(f, "video", 1, 0, true, 0, s.asize, true);
        cycles(f, 4);
    } else {
        write_loc_eog(f, "video", 0, 1, false, 0, s.asize, true);
        cycles(f, 4);
        terminal_normal_done(f, ver, "video");
        REQUIRE(until_receiver_ended(f, "video"));
    }
    ev_log_t evl = {0};
    out_log_t log = {0};
    shared_poll(&s, 2, &log, &evl);
    REQUIRE(shared_hold_genuine(&s, &evl, &log));
    CHECK(moq_media_player_next_deadline_us(f->p) == UINT64_MAX);
    f->now += 3 * TAIL_US;                   /* the caller sleeps, no tick */
    pause_decoder(f, s.ab, s.audio_gen, 0);
    uint64_t resumed = f->now;
    uint64_t due = moq_media_player_next_deadline_us(f->p);
    CHECK(due > resumed + TAIL_US / 2 && due <= resumed + TAIL_US);
    REQUIRE(due != UINT64_MAX && due > resumed + 2000);
    (void)cycle(f);
    outputs(f, &log);
    events(f, &evl);
    CHECK(pstats(f).held_objects == 0);
    CHECK(moq_media_player_next_deadline_us(f->p) == due);
    (void)cycle_to(f, due - 1000);
    outputs(f, &log);
    events(f, &evl);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, s.va) == 0);
    (void)cycle_to(f, due);
    shared_poll(&s, 8, &log, &evl);
    static const uint64_t a_eot[][2] = { { 1, 0 } };
    static const uint64_t a_done[][2] = { { 0, 1 } };
    expect_units(&log, s.va, eot ? a_eot : a_done, 1, true);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, s.va) == 1);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL && e->track == s.va);
    CHECK(e->u.end.cause == (eot ? MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK
                                 : MOQ_MEDIA_PLAYER_END_PEER_DONE));
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF);
out:
    pf_teardown(&s.f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* An END_OF_TRACK's tail wait does not count while the engine has a
 * command the application's unpolled output leaves no room for, with
 * nothing held: the outputs left unpolled from the start, the END_OF_TRACK
 * (nothing at or past (1, 4)) arriving before group 1's delayed stream,
 * group 1's units queued in the engine behind the full output queue. The
 * caller sleeps three tail waits, then polls and ticks until the engine
 * has nothing left behind the queue: none of the sleep counted, the
 * deadline lies ahead by the whole wait from that tick. With `complete`,
 * every unit comes out once, in order, and the track ends
 * END{PEER_END_OF_TRACK, COMPLETE}; without, (1, 3) never comes, and the
 * track ends exactly at that deadline, END{PEER_END_OF_TRACK, TAIL_CUTOFF},
 * after (1, 2). */
static void test_eot_bare_backlog(moq_version_t ver, bool complete)
{
    static char label[64];
    snprintf(label, sizeof(label), "eot_bare_backlog_%s_%s", complete ? "end" : "cutoff",
             vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .gap_timeout_us = UINT64_MAX, .max_outputs = 2 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video");
    cycles(&f, 16);
    write_loc_eog(&f, "video", 0, 0, true, 0, 0, true);
    end_group(&f, "video");
    cycles(&f, 8);                           /* outputs left unpolled */
    REQUIRE(moq_media_receiver_test_end_of_track(f.r, v, 1, 4));
    cycles(&f, 2);
    uint64_t last = complete ? 4 : 3;
    for (uint64_t ob = 0; ob < last; ob++)
        write_loc_eog(&f, "video", 1, ob, ob == 0, 0, 0, complete);
    if (complete) end_group(&f, "video");
    cycles(&f, 4);
    REQUIRE(pstats(&f).held_objects == 0);
    /* The CONFIGURE is queued; the first unit, needing its TIMELINE too,
     * waits in the engine. */
    REQUIRE(pstats(&f).outputs_queued == 1);
    REQUIRE(pstats(&f).units_output == 0);
    CHECK(pstats(&f).ends_output == 0);
    CHECK(moq_media_player_next_deadline_us(f.p) == UINT64_MAX);
    f.now += 3 * TAIL_US;                    /* the caller sleeps, no tick */
    out_log_t log = {0};
    uint64_t released = 0, due = UINT64_MAX;
    for (unsigned i = 0; i < 32 && due == UINT64_MAX && pstats(&f).ends_output == 0; i++) {
        outputs(&f, &log);
        (void)cycle(&f);
        released = f.now;
        due = moq_media_player_next_deadline_us(f.p);
    }
    if (complete) {
        for (unsigned i = 0; i < 8; i++) {
            outputs(&f, &log);
            (void)cycle(&f);
            events(&f, NULL);
        }
        outputs(&f, &log);
        CHECK(f.now < released + TAIL_US);
    } else {
        CHECK(due > released + TAIL_US / 2 && due <= released + TAIL_US);
        REQUIRE(due != UINT64_MAX && due > f.now + 2000);
        outputs(&f, &log);
        (void)cycle(&f);
        outputs(&f, &log);
        CHECK(moq_media_player_next_deadline_us(f.p) == due);
        (void)cycle_to(&f, due - 1000);
        outputs(&f, &log);
        CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, v) == 0);
        (void)cycle_to(&f, due);
        for (unsigned i = 0; i < 8; i++) {
            (void)cycle(&f);
            outputs(&f, &log);
            events(&f, NULL);
        }
    }
    static const uint64_t ids[][2] = { { 0, 0 }, { 1, 0 }, { 1, 1 }, { 1, 2 }, { 1, 3 } };
    expect_units(&log, v, ids, complete ? 5 : 4, true);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, v) == 1);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
    CHECK(e->u.end.disposition == (complete ? MOQ_MEDIA_PLAYER_END_COMPLETE
                                            : MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF));
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

typedef enum { STAGED_DECODE_ERROR, STAGED_DESELECT, STAGED_FAILURE, STAGED_DESTROY } staged_end_t;

/* The command the output queue had no room for is owned exactly once, and
 * its track's end never overtakes it: a two-slot output queue left
 * unpolled, the track's CONFIGURE queued and its first unit (100 bytes)
 * taken from the engine under its hold, waiting for room.
 * - A decode error makes it moot like the queued outputs: its hold ends at
 *   once and it never comes out.
 * - Deselecting the track ends its hold at once.
 * - A local failure ends the track only after it: the unit comes out, then
 *   END{LOCAL_FAILURE, IMMEDIATE}.
 * - Destroying the player with it staged frees it (sanitizer lanes). */
static void test_staged_ownership(moq_version_t ver, staged_end_t how)
{
    static const char *const names[] = { "decode_error", "deselect", "failure", "destroy" };
    static char label[64];
    snprintf(label, sizeof(label), "staged_%s_%s", names[how], vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .gap_timeout_us = UINT64_MAX, .max_outputs = 2 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video");
    moq_playback_t *pb = moq_media_player_test_engine(f.p);
    cycles(&f, 16);
    write_loc_eog(&f, "video", 0, 0, true, 0, 100, false);
    cycles(&f, 8);
    REQUIRE(pstats(&f).outputs_queued == 1 && pstats(&f).units_output == 0);
    uint64_t held = moq_playback_command_hold_bytes(pb);
    REQUIRE(held >= 100);
    out_log_t log = {0};
    switch (how) {
    case STAGED_DECODE_ERROR: {
        moq_media_player_output_t c;
        REQUIRE(moq_media_player_poll_output(f.p, &c, sizeof(c)) == MOQ_OK);
        REQUIRE(c.kind == MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE);
        uint64_t gen = c.configuration_generation;
        moq_media_player_output_cleanup(&c);
        CHECK(moq_playback_command_hold_bytes(pb) == 100);   /* the staged unit */
        moq_media_player_feedback_t err;
        moq_media_player_feedback_init(&err);
        err.kind = MOQ_MEDIA_PLAYER_FEEDBACK_DECODE_ERROR;
        err.track = v;
        err.configuration_generation = gen;
        CHECK(moq_media_player_handle_feedback(f.p, &err, f.now) == MOQ_OK);
        CHECK(moq_playback_command_hold_bytes(pb) == 0);
        for (unsigned i = 0; i < 8; i++) {
            (void)cycle(&f);
            outputs(&f, &log);
        }
        CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, v) == 0);
        CHECK(pstats(&f).units_output == 0);
        break;
    }
    case STAGED_DESELECT:
        CHECK(moq_media_player_deselect_track(f.p, v) == MOQ_OK);
        CHECK(moq_playback_command_hold_bytes(pb) == held - 100);
        for (unsigned i = 0; i < 4; i++) {
            (void)cycle(&f);
            outputs(&f, &log);
        }
        CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, NULL) == 0);
        CHECK(pstats(&f).units_output == 0);
        break;
    case STAGED_FAILURE: {
        moq_media_receiver_test_fail_track(f.r, v);
        for (unsigned i = 0; i < 8; i++) {
            (void)cycle(&f);
            outputs(&f, &log);
            events(&f, NULL);
        }
        static const uint64_t ids[][2] = { { 0, 0 } };
        expect_units(&log, v, ids, 1, true);
        const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
        REQUIRE(e != NULL && e == &log.o[log.n - 1]);
        CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE);
        CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_IMMEDIATE);
        break;
    }
    case STAGED_DESTROY:
        break;
    }
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A full output queue with the engine holding nothing is no backlog: the
 * track's first outputs fill all but one slot, unpolled; the END_OF_TRACK
 * (nothing at or past (1, 2)) arrives and group 1 never does. The tail wait
 * runs from the END_OF_TRACK: no END a millisecond before its deadline, and
 * at it END{PEER_END_OF_TRACK, TAIL_CUTOFF} goes into the last free slot
 * without the application polling anything. */
static void test_eot_full_queue_no_backlog(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "eot_full_queue_no_backlog_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .gap_timeout_us = UINT64_MAX, .max_outputs = 4 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video");
    cycles(&f, 16);
    write_loc_eog(&f, "video", 0, 0, true, 0, 0, true);
    end_group(&f, "video");
    cycles(&f, 8);                           /* outputs left unpolled */
    REQUIRE(pstats(&f).outputs_queued == 3);
    REQUIRE(moq_media_receiver_test_end_of_track(f.r, v, 1, 2));
    (void)cycle(&f);
    uint64_t armed = f.now;
    uint64_t due = moq_media_player_next_deadline_us(f.p);
    CHECK(due != UINT64_MAX && due <= armed + TAIL_US && due > armed + TAIL_US / 2);
    REQUIRE(due != UINT64_MAX && due > f.now + 2000);
    (void)cycle_to(&f, due - 1000);
    CHECK(pstats(&f).ends_output == 0);
    (void)cycle_to(&f, due);
    for (unsigned i = 0; i < 4; i++) (void)cycle(&f);
    CHECK(pstats(&f).ends_output == 1);
    CHECK(pstats(&f).outputs_queued == 4);
    out_log_t log = {0};
    outputs(&f, &log);
    static const uint64_t ids[][2] = { { 0, 0 } };
    expect_units(&log, v, ids, 1, true);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL && e == &log.o[log.n - 1]);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* An END_OF_TRACK's tail wait does not count while the track's own
 * decoder is paused, with nothing held: the decoder paused, the
 * END_OF_TRACK (nothing at or past (1, 2)) arriving before group 1's
 * delayed stream, (1, 0) and, with `complete`, (1, 1) and the group's end
 * buffered in the engine. The caller sleeps three tail waits with no tick, then resumes the
 * decoder: the next deadline lies ahead by the whole wait (none of the
 * pause counted). With `complete`, both units come out and the track ends
 * END{PEER_END_OF_TRACK, COMPLETE}; without, (1, 1) never comes, and the
 * track ends exactly at that deadline: no END a millisecond before it, one
 * END{PEER_END_OF_TRACK, TAIL_CUTOFF} at it, after (1, 0). */
static void test_eot_own_pause(moq_version_t ver, bool complete)
{
    static char label[64];
    snprintf(label, sizeof(label), "eot_own_pause_%s_%s", complete ? "end" : "cutoff",
             vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .gap_timeout_us = UINT64_MAX };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video");
    cycles(&f, 16);
    write_loc_eog(&f, "video", 0, 0, true, 0, 0, true);
    end_group(&f, "video");
    cycles(&f, 8);
    out_log_t log = {0};
    outputs(&f, &log);
    const moq_media_player_output_t *u = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT);
    REQUIRE(u != NULL);
    uint64_t gen = u->configuration_generation;
    pause_decoder(&f, v, gen, 9);
    REQUIRE(moq_media_receiver_test_end_of_track(f.r, v, 1, 2));
    cycles(&f, 2);
    write_loc_eog(&f, "video", 1, 0, true, 0, 0, complete);
    if (complete) {
        write_loc_eog(&f, "video", 1, 1, false, 0, 0, true);
        end_group(&f, "video");
    }
    for (unsigned i = 0; i < 4; i++) {
        (void)cycle(&f);
        outputs(&f, &log);
        events(&f, NULL);
    }
    REQUIRE(pstats(&f).held_objects == 0);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, v) == 0);
    CHECK(moq_media_player_next_deadline_us(f.p) == UINT64_MAX);
    f.now += 3 * TAIL_US;                    /* the caller sleeps, no tick */
    pause_decoder(&f, v, gen, 0);
    uint64_t resumed = f.now;
    uint64_t due = moq_media_player_next_deadline_us(f.p);
    CHECK(due > resumed + TAIL_US / 2 && due <= resumed + TAIL_US);
    REQUIRE(due != UINT64_MAX && due > resumed + 2000);
    if (complete) {
        for (unsigned i = 0; i < 8; i++) {
            (void)cycle(&f);
            outputs(&f, &log);
            events(&f, NULL);
        }
        CHECK(f.now < due);
    } else {
        (void)cycle(&f);
        outputs(&f, &log);
        CHECK(moq_media_player_next_deadline_us(f.p) == due);
        (void)cycle_to(&f, due - 1000);
        outputs(&f, &log);
        CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, v) == 0);
        (void)cycle_to(&f, due);
        for (unsigned i = 0; i < 8; i++) {
            (void)cycle(&f);
            outputs(&f, &log);
            events(&f, NULL);
        }
    }
    static const uint64_t both[][2] = { { 0, 0 }, { 1, 0 }, { 1, 1 } };
    expect_units(&log, v, both, complete ? 3 : 2, true);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, v) == 1);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
    CHECK(e->u.end.disposition == (complete ? MOQ_MEDIA_PLAYER_END_COMPLETE
                                            : MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF));
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* An END_OF_TRACK's tail wait does not count while input is held behind
 * output the application has not polled: the END_OF_TRACK (nothing at or
 * past (1, 4)) arrives before group 1's delayed stream, the outputs are
 * left unpolled, and the units queued behind them fill the engine's byte
 * budget, so group 1's second object is held. The caller sleeps three tail
 * waits, then polls and ticks: every unit comes out once, in order, and
 * the track ends END{PEER_END_OF_TRACK, COMPLETE}. */
static void test_eot_full_output(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "eot_full_output_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .gap_timeout_us = UINT64_MAX, .max_buffered_bytes = 250,
                    .max_outputs = 2 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video");
    cycles(&f, 16);
    write_loc_eog(&f, "video", 0, 0, true, 0, 100, true);
    end_group(&f, "video");
    cycles(&f, 8);                           /* outputs left unpolled */
    REQUIRE(moq_media_receiver_test_end_of_track(f.r, v, 1, 4));
    cycles(&f, 2);
    for (uint64_t ob = 0; ob < 4; ob++) write_loc_eog(&f, "video", 1, ob, ob == 0, 0, 100, true);
    end_group(&f, "video");
    cycles(&f, 4);
    REQUIRE(pstats(&f).held_objects == 1);
    REQUIRE(pstats(&f).outputs_queued > 0);
    CHECK(moq_media_player_next_deadline_us(f.p) == UINT64_MAX);
    f.now += 3 * TAIL_US;                    /* the caller sleeps, no tick */
    out_log_t log = {0};
    for (unsigned i = 0; i < 2 * (TAIL_US / 1000); i++) {
        outputs(&f, &log);
        (void)cycle(&f);
        events(&f, NULL);
    }
    outputs(&f, &log);
    static const uint64_t ids[][2] = { { 0, 0 }, { 1, 0 }, { 1, 1 }, { 1, 2 }, { 1, 3 } };
    expect_units(&log, v, ids, 5, true);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_COMPLETE);
    CHECK(pstats(&f).held_objects == 0);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* Only an END_OF_TRACK's tail is held up by the hold: an error done's tail
 * wait runs regardless. With A's object held behind B's paused media, the
 * error done's wait passes during the pause; once B resumes and the held
 * unit goes in, A ends at once, END{PEER_DONE, TAIL_CUTOFF}, with no wait
 * left. */
static void test_shared_capacity_error_done(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "shared_capacity_error_done_%s", vname(ver));
    row = label;
    unsigned before = failures;
    shared_t s;
    REQUIRE(shared_setup(&s, ver, false));
    pf_t *f = &s.f;
    write_loc_eog(f, "video", 0, 1, false, 0, s.asize, true);
    ev_log_t evl = {0};
    out_log_t log = {0};
    shared_poll(&s, 4, &log, &evl);
    REQUIRE(shared_hold_genuine(&s, &evl, &log));
    CHECK(moq_pub_finish_subscribers(f->pub, pf_pub(f, "video"), 0x4u, f->now) == MOQ_OK);
    shared_poll(&s, 3 * (TAIL_US / 1000), &log, &evl);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, s.va) == 0);
    CHECK(pstats(f).held_objects == 1);
    pause_decoder(f, s.ab, s.audio_gen, 0);
    shared_poll(&s, 8, &log, &evl);
    static const uint64_t a_ids[][2] = { { 0, 1 } };
    expect_units(&log, s.va, a_ids, 1, true);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, s.va) == 1);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL && e->track == s.va);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_DONE);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF);
out:
    pf_teardown(&s.f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A cause that allows no wait takes no wait: with A's object held behind
 * B's paused media, a local failure of A settles what the player holds
 * first, so A ends once B resumes and the held unit goes in -- at once,
 * END{LOCAL_FAILURE, IMMEDIATE} right after that unit, with no tail wait,
 * however long B stayed paused. */
static void test_shared_capacity_failure(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "shared_capacity_failure_%s", vname(ver));
    row = label;
    unsigned before = failures;
    shared_t s;
    REQUIRE(shared_setup(&s, ver, false));
    pf_t *f = &s.f;
    write_loc_eog(f, "video", 0, 1, false, 0, s.asize, true);
    ev_log_t evl = {0};
    out_log_t log = {0};
    shared_poll(&s, 4, &log, &evl);
    REQUIRE(shared_hold_genuine(&s, &evl, &log));
    moq_media_receiver_test_fail_track(f->r, s.va);
    shared_poll(&s, 3 * (TAIL_US / 1000), &log, &evl);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, s.va) == 0);
    CHECK(pstats(f).held_objects == 1);
    pause_decoder(f, s.ab, s.audio_gen, 0);
    shared_poll(&s, 8, &log, &evl);
    static const uint64_t a_ids[][2] = { { 0, 1 } };
    expect_units(&log, s.va, a_ids, 1, true);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, s.va) == 1);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL && e->track == s.va);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_IMMEDIATE);
out:
    pf_teardown(&s.f);
    if (failures == before) printf("PASS: %s\n", row);
}

typedef enum {
    END_BY_CLOSE, END_BY_FATAL, END_BY_LOCAL_FAILURE, END_BY_NORMAL_DONE
} end_by_t;

static const char *end_by_name(end_by_t by)
{
    return by == END_BY_CLOSE ? "closed" : by == END_BY_FATAL ? "fatal"
         : by == END_BY_LOCAL_FAILURE ? "local_failure" : "normal_done";
}

static void end_by(pf_t *f, end_by_t by, moq_media_track_t *t, moq_version_t ver)
{
    if (by == END_BY_CLOSE) moq_endpoint_test_set_stopped(f->ep);
    else if (by == END_BY_FATAL) moq_media_receiver_test_set_fatal(f->r, 1);
    else if (by == END_BY_LOCAL_FAILURE) moq_media_receiver_test_fail_track(f->r, t);
    else terminal_normal_done(f, ver, "video");
}

/* A cause that allows no wait settles what the player holds: with (0, 1)
 * missing for good, (0, 2) and (0, 3) in a two-object engine and (0, 4)
 * held, closure, a fatal receiver or a local failure releases (0, 2) to
 * (0, 4) in decode order, the held one included, then one END{cause,
 * IMMEDIATE}. A terminal normal done does the same once its tail wait
 * passes, as END{PEER_DONE, TAIL_CUTOFF}. Under output pressure (one output
 * polled per pass) too. */
static void test_terminal_settles_held(moq_version_t ver, end_by_t by, bool pressure)
{
    static char label[96];
    snprintf(label, sizeof(label), "terminal_settles_held_%s_%s%s", end_by_name(by),
             vname(ver), pressure ? "_pressure" : "");
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .gap_timeout_us = UINT64_MAX, .max_buffered_objects = 2,
                    .max_outputs = pressure ? 2 : 0 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video");
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    for (unsigned i = 0; i < 8; i++) {
        (void)cycle(&f);
        outputs(&f, NULL);
        events(&f, NULL);
    }
    /* (0, 2) is independent, so the track can resume there once (0, 1) is
     * given up. */
    for (uint64_t ob = 2; ob <= 4; ob++) write_loc(&f, "video", 0, ob, ob == 2, 0, 0);
    cycles(&f, 4);
    REQUIRE(pstats(&f).held_objects == 1);
    end_by(&f, by, v, ver);
    out_log_t log = {0};
    unsigned passes = by == END_BY_NORMAL_DONE ? 40 + 3 * (unsigned)(TAIL_US / 1000) : 40;
    for (unsigned i = 0; i < passes; i++) {
        (void)cycle(&f);
        if (pressure) {
            moq_media_player_output_t x;
            if (moq_media_player_poll_output(f.p, &x, sizeof(x)) == MOQ_OK) {
                if (log.n < 96) {
                    log.o[log.n] = x;
                    log.o[log.n].u.unit.payload = NULL;
                    log.o[log.n].u.configure.codec_config = NULL;
                    log.n++;
                }
                moq_media_player_output_cleanup(&x);
            }
        } else {
            outputs(&f, &log);
        }
        events(&f, NULL);
    }
    outputs(&f, &log);
    CHECK(pstats(&f).held_objects == 0);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, NULL) == 1);
    uint64_t want = 2;
    bool end_seen = false;
    for (size_t i = 0; i < log.n; i++) {
        if (log.o[i].kind == MOQ_MEDIA_PLAYER_OUTPUT_END) {
            end_seen = true;
            uint32_t cause = by == END_BY_CLOSE ? MOQ_MEDIA_PLAYER_END_CLOSED
                           : by == END_BY_FATAL ? MOQ_MEDIA_PLAYER_END_FATAL
                           : by == END_BY_LOCAL_FAILURE ? MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE
                                                        : MOQ_MEDIA_PLAYER_END_PEER_DONE;
            CHECK(log.o[i].u.end.cause == cause);
            CHECK(log.o[i].u.end.disposition == (by == END_BY_NORMAL_DONE
                      ? MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF : MOQ_MEDIA_PLAYER_END_IMMEDIATE));
        } else if (log.o[i].kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT) {
            CHECK(!end_seen);
            CHECK(log.o[i].u.unit.object_id == want);
            want++;
        }
    }
    CHECK(want == 5);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* The highest cause before END wins, also once a wait has begun and once
 * the END waits for room: closure during an END_OF_TRACK tail, a fatal
 * receiver during an error done's tail, and closure while a complete END
 * is owed behind a full output queue all end IMMEDIATE with the higher
 * cause. */
static void test_cause_upgrades(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "cause_upgrades_%s", vname(ver));
    row = label;
    unsigned before = failures;
    for (int c = 0; c < 3; c++) {
        pf_opts_t o = { .gap_timeout_us = 10000000, .max_outputs = c == 2 ? 2 : 0 };
        pf_t f;
        REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
        REQUIRE(select_plain(&f, "video") == MOQ_OK);
        cycles(&f, 16);
        write_loc_eog(&f, "video", 0, 0, true, 0, 0, c == 2);
        uint32_t want = MOQ_MEDIA_PLAYER_END_CLOSED;
        if (c == 0) {
            write_loc(&f, "video", 0, 2, false, 0, 0);
            CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
            cycles(&f, 4);
            outputs(&f, NULL);
            CHECK(moq_media_player_next_deadline_us(f.p) != UINT64_MAX);
            moq_endpoint_test_set_stopped(f.ep);
        } else if (c == 1) {
            end_group(&f, "video");
            cycles(&f, 8);
            CHECK(moq_pub_finish_subscribers(f.pub, pf_pub(&f, "video"), 0x4u, f.now) == MOQ_OK);
            cycles(&f, 4);
            outputs(&f, NULL);
            CHECK(moq_media_player_next_deadline_us(f.p) != UINT64_MAX);
            moq_media_receiver_test_set_fatal(f.r, 7);
            want = MOQ_MEDIA_PLAYER_END_FATAL;
        } else {
            write_loc_eog(&f, "video", 0, 1, false, 0, 0, true);
            end_group(&f, "video");
            CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
            for (unsigned i = 0; i < 12; i++) (void)cycle(&f);   /* outputs full */
            moq_endpoint_test_set_stopped(f.ep);
        }
        out_log_t log = {0};
        for (unsigned i = 0; i < 40; i++) {
            (void)cycle(&f);
            outputs(&f, &log);
            events(&f, NULL);
        }
        const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
        CHECK(e != NULL);
        if (e) {
            CHECK(e->u.end.cause == want);
            CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_IMMEDIATE);
        }
        pf_teardown(&f);
    }
out:
    if (failures == before) printf("PASS: %s\n", row);
}

/* A unit the player itself drops after the end began is loss: two AAC
 * units without configuration, their group end and END_OF_TRACK in one
 * batch give no unit and an END that is not COMPLETE. */
static void test_player_drop_is_loss(void)
{
    row = "player_drop_is_loss";
    unsigned before = failures;
    static const char *const names[] = { "aac" };
    static const char cat[] = CATALOG(LOC_AUDIO("aac", "mp4a.40.2", ""));
    pf_t f;
    REQUIRE(pf_setup(&f, MOQ_VERSION_DRAFT_22, cat, names, 1, NULL));
    REQUIRE(select_plain(&f, "aac") == MOQ_OK);
    cycles(&f, 16);
    write_loc_eog(&f, "aac", 0, 0, false, 0, 0, true);
    write_loc_eog(&f, "aac", 0, 1, false, 0, 0, true);
    CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "aac"), f.now) == MOQ_OK);
    out_log_t log = {0};
    for (unsigned i = 0; i < 80; i++) {
        (void)cycle(&f);
        outputs(&f, &log);
        events(&f, NULL);
    }
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, NULL) == 0);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.disposition != MOQ_MEDIA_PLAYER_END_COMPLETE);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A gap settled before END_OF_TRACK stays before it however late its
 * report is read: with a two-slot output queue left unpolled across the
 * gap's settlement and the END_OF_TRACK, the end is still COMPLETE. */
static void test_gap_settled_before_end(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "gap_settled_before_end_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .max_outputs = 2 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    write_loc(&f, "video", 0, 2, false, 0, 0);
    end_group(&f, "video");
    write_loc(&f, "video", 1, 0, true, 0, 0);
    end_group(&f, "video");
    for (unsigned i = 0; i < 30; i++) (void)cycle(&f);   /* nothing polled */
    CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
    cycles(&f, 8);
    out_log_t log = {0};
    ev_log_t evl = {0};
    for (unsigned i = 0; i < 40; i++) {
        (void)cycle(&f);
        outputs(&f, &log);
        events(&f, &evl);
    }
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_GAP) + count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_SKIP) >= 1);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_COMPLETE);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A GAP announcement is not loss: after END_OF_TRACK at (0, 3) starts the
 * end, the engine announces its wait on (0, 1), and with one engine event slot and
 * the output queue full, the step that would give (0, 1) up waits for room.
 * (0, 1) arrives first, so nothing is given up: every unit comes out, no
 * SKIP, and the end is COMPLETE. */
static void test_gap_announced_then_filled(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "gap_announced_then_filled_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .max_outputs = 2, .engine_max_events = 1 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    write_loc(&f, "video", 0, 2, false, 0, 0);
    cycles(&f, 2);
    REQUIRE(moq_media_receiver_test_end_of_track(f.r, pf_track(&f, "video"), 0, 3));
    ev_log_t evl = {0};
    bool announced = false;
    for (unsigned i = 0; i < 40 && !announced; i++) {
        (void)cycle(&f);
        events(&f, &evl);
        announced = count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_GAP) > 0;
    }
    REQUIRE(announced);
    /* (0, 1) by another route (its subgroup's stream is past it). */
    moq_rcbuf_t *pay = NULL;
    static const uint8_t bytes[8] = { 1, 2, 3 };
    REQUIRE(moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes), &pay) == MOQ_OK);
    bool injected = moq_media_receiver_test_inject_object(f.r, pf_track(&f, "video"), 0, 1,
                                                          false, ts_of(0, 1), pay);
    moq_rcbuf_decref(pay);
    REQUIRE(injected);
    out_log_t log = {0};
    for (unsigned i = 0; i < 40; i++) {
        (void)cycle(&f);
        outputs(&f, &log);
        events(&f, &evl);
    }
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, NULL) == 3);
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_SKIP) == 0);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_COMPLETE);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A unit given up inside its group is loss when it comes after the end
 * began: independent (0, 0) and (0, 2), (0, 1) missing, the group's FIN
 * clean with END_OF_GROUP. With END_OF_TRACK in the same batch, release
 * resumes at (0, 2) behind a DISCONTINUITY and the end is TAIL_CUTOFF.
 * Given up before END_OF_TRACK arrives, it is settled history: COMPLETE. */
static void test_in_group_skip_and_end(moq_version_t ver, bool skip_first)
{
    static char label[64];
    snprintf(label, sizeof(label), "in_group_skip_%s_%s",
             skip_first ? "before_end" : "after_end", vname(ver));
    row = label;
    unsigned before = failures;
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc_eog(&f, "video", 0, 0, true, 0, 0, true);
    write_loc_eog(&f, "video", 0, 2, true, 0, 0, true);
    out_log_t log = {0};
    ev_log_t evl = {0};
    if (skip_first) {
        for (unsigned i = 0; i < 30; i++) {
            (void)cycle(&f);
            outputs(&f, &log);
            events(&f, &evl);
        }
        REQUIRE(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, NULL) == 2);
    }
    CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
    for (unsigned i = 0; i < 80; i++) {
        (void)cycle(&f);
        outputs(&f, &log);
        events(&f, &evl);
    }
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, NULL) == 2);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_DISCONTINUITY, NULL) == 1);
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_SKIP) == 0);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
    CHECK(e->u.end.disposition == (skip_first ? MOQ_MEDIA_PLAYER_END_COMPLETE
                                              : MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF));
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* AUTO considers every member of an alternate group: 65 renditions in one
 * group give one selected track, and a preferred language past index 63
 * is found. */
static void test_auto_large_alternate_group(void)
{
    row = "auto_large_alternate_group";
    unsigned before = failures;
    size_t cap = 65 * 220 + 64;
    char *cat = (char *)malloc(cap);
    REQUIRE(cat != NULL);
    size_t n = (size_t)snprintf(cat, cap, "{\"version\":1,\"tracks\":[");
    for (int i = 0; i < 65; i++)
        n += (size_t)snprintf(cat + n, cap - n,
            "%s{\"name\":\"v%d\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\","
            "\"codec\":\"avc1.42e01e\",\"renderGroup\":1,\"altGroup\":1,\"bitrate\":%d%s}",
            i ? "," : "", i, 1000 + i, i == 64 ? ",\"lang\":\"fr\"" : "");
    snprintf(cat + n, cap - n, "]}");
    static const char *const names[] = { "v64" };
    pf_opts_t o = { .selection = MOQ_MEDIA_SELECTION_AUTO, .max_events = 128, .lang = "fr",
                    .max_track_events = 128 };
    pf_t f;
    bool ok = pf_setup(&f, MOQ_VERSION_DRAFT_18, cat, names, 1, &o);
    free(cat);
    REQUIRE(ok);
    cycles(&f, 8);
    events(&f, NULL);
    CHECK(pstats(&f).tracks_selected == 1);
    /* The selected track is the French one: its media plays. */
    write_loc(&f, "v64", 0, 0, true, 0, 0);
    out_log_t log = {0};
    for (unsigned i = 0; i < 16; i++) {
        (void)cycle(&f);
        outputs(&f, &log);
    }
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, NULL) == 1);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A track event may pass a held object and start the end, but the END
 * follows it: a local failure while (0, 3) is held, behind a decoder pause,
 * ends nothing while the pause holds release; once lifted, (0, 1) to (0, 3)
 * come out, the held one last, then END{LOCAL_FAILURE, IMMEDIATE}. */
static void test_held_then_local_failure(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "held_then_local_failure_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .max_buffered_objects = 2 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video");
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    cycles(&f, 8);
    out_log_t log = {0};
    outputs(&f, &log);
    pause_decoder(&f, v, last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT)->configuration_generation, 9);
    for (uint64_t ob = 1; ob <= 3; ob++) write_loc(&f, "video", 0, ob, false, 0, 0);
    cycles(&f, 4);
    REQUIRE(pstats(&f).held_objects == 1);
    moq_media_receiver_test_fail_track(f.r, v);
    out_log_t after = {0};
    for (unsigned i = 0; i < 16; i++) {
        (void)cycle(&f);
        outputs(&f, &after);
        events(&f, NULL);
    }
    /* The decoder's own pause still holds release, so nothing ends yet. */
    CHECK(count_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_END, NULL) == 0);
    pause_decoder(&f, v, last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT)->configuration_generation, 0);
    for (unsigned i = 0; i < 24; i++) {
        (void)cycle(&f);
        outputs(&f, &after);
        events(&f, NULL);
    }
    CHECK(count_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, v) == 3);
    const moq_media_player_output_t *e = last_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e == &after.o[after.n - 1]);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_IMMEDIATE);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A local failure without TRACK_ENDED ends the track: what it holds is
 * released, then END{LOCAL_FAILURE, IMMEDIATE}; the other track plays on. */
static void test_local_failure(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "local_failure_%s", vname(ver));
    row = label;
    unsigned before = failures;
    static const char *const names[] = { "video", "audio" };
    static const char cat[] = CATALOG(LOC_VIDEO("video", "") "," LOC_AUDIO("audio", "opus", ""));
    pf_opts_t o = { .gap_timeout_us = UINT64_MAX };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, cat, names, 2, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    REQUIRE(select_plain(&f, "audio") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video"), *a = pf_track(&f, "audio");
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    write_loc(&f, "video", 0, 2, true, 0, 0);
    write_loc(&f, "audio", 0, 0, true, 0, 0);
    cycles(&f, 8);
    moq_media_receiver_test_fail_track(f.r, v);
    write_loc(&f, "audio", 0, 1, true, 0, 0);
    out_log_t log = {0};
    ev_log_t evl = {0};
    for (unsigned i = 0; i < 24; i++) {
        (void)cycle(&f);
        outputs(&f, &log);
        events(&f, &evl);
    }
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, v) == 2);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, a) == 2);
    size_t ends = 0;
    for (size_t i = 0; i < log.n; i++) {
        if (log.o[i].kind != MOQ_MEDIA_PLAYER_OUTPUT_END) continue;
        ends++;
        CHECK(log.o[i].track == v);
        CHECK(log.o[i].u.end.cause == MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE);
        CHECK(log.o[i].u.end.disposition == MOQ_MEDIA_PLAYER_END_IMMEDIATE);
    }
    CHECK(ends == 1);
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_ENDED) == 0);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* Input after END is settled at the boundary: an object of an ended track
 * is dropped LATE_AFTER_END, counted, and never becomes a unit. */
static void test_late_after_end(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "late_after_end_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, NULL));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video");
    cycles(&f, 16);
    write_loc_eog(&f, "video", 0, 0, true, 0, 0, true);
    end_group(&f, "video");
    CHECK(moq_pub_end_track(f.pub, pf_pub(&f, "video"), f.now) == MOQ_OK);
    out_log_t log = {0};
    for (unsigned i = 0; i < 24; i++) {
        (void)cycle(&f);
        outputs(&f, &log);
        events(&f, NULL);
    }
    REQUIRE(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, v) == 1);
    uint64_t dropped = pstats(&f).objects_dropped;
    moq_rcbuf_t *pay = NULL;
    static const uint8_t bytes[8] = { 1, 2, 3 };
    REQUIRE(moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes), &pay) == MOQ_OK);
    CHECK(moq_media_receiver_test_inject_object(f.r, v, 1, 0, true, ts_of(1, 0), pay));
    moq_rcbuf_decref(pay);
    out_log_t late = {0};
    ev_log_t evl = {0};
    for (unsigned i = 0; i < 8; i++) {
        (void)cycle(&f);
        outputs(&f, &late);
        events(&f, &evl);
    }
    CHECK(count_out(&late, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, NULL) == 0);
    size_t late_drops = 0;
    for (size_t i = 0; i < evl.n; i++)
        late_drops += evl.ev[i].kind == MOQ_MEDIA_PLAYER_EVENT_DROP &&
                      evl.ev[i].u.drop.reason == MOQ_MEDIA_PLAYER_DROP_LATE_AFTER_END &&
                      evl.ev[i].u.drop.group_id == 1;
    CHECK(late_drops == 1);
    CHECK(pstats(&f).objects_dropped == dropped + 1);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* END_OF_TRACK can arrive before the first object of a stream still in
 * flight: it ends nothing early. The objects that arrive after it, within
 * the tail, are played, and the end is then COMPLETE. */
static void test_end_before_delayed_stream(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "end_before_delayed_stream_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .gap_timeout_us = 10000000 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video");
    cycles(&f, 16);
    write_loc_eog(&f, "video", 0, 0, true, 0, 0, true);
    end_group(&f, "video");
    cycles(&f, 8);
    out_log_t log = {0};
    outputs(&f, &log);
    /* No object at or past (1, 2) exists; group 1 is still in flight. */
    REQUIRE(moq_media_receiver_test_end_of_track(f.r, v, 1, 2));
    cycles(&f, 4);
    outputs(&f, &log);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, NULL) == 0);
    write_loc(&f, "video", 1, 0, true, 0, 0);
    write_loc(&f, "video", 1, 1, false, 0, 0);
    for (unsigned i = 0; i < 16; i++) {
        (void)cycle(&f);
        outputs(&f, &log);
        events(&f, NULL);
    }
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, v) == 3);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e == &log.o[log.n - 1]);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK);
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_COMPLETE);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* wait sleeps for real: with nothing to do it returns DONE after its
 * timeout; an interrupt raised during the sleep returns INTERRUPTED at
 * once; a deadline earlier than the timeout ends the sleep with OK at the
 * deadline. */
typedef struct { moq_endpoint_t *ep; } interrupter_t;

static void *interrupt_later(void *arg)
{
    interrupter_t *it = (interrupter_t *)arg;
    struct timespec ts = { 0, 30 * 1000 * 1000 };
    nanosleep(&ts, NULL);
    moq_endpoint_set_interrupted(it->ep, true);
    return NULL;
}

static void test_wait_sleeps(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "wait_sleeps_%s", vname(ver));
    row = label;
    unsigned before = failures;
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, NULL));
    moq_endpoint_test_set_waitable(f.ep);
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    end_group(&f, "video");
    cycles(&f, 8);
    outputs(&f, NULL);
    events(&f, NULL);
    CHECK(moq_media_player_tick(f.p, f.now) == MOQ_OK);
    outputs(&f, NULL);
    events(&f, NULL);
    /* A wake retained from earlier activity may end one wait at once. */
    (void)moq_media_player_wait(f.p, f.now, 0);

    uint64_t t0 = mono_us();
    CHECK(moq_media_player_wait(f.p, f.now, 60000) == MOQ_DONE);
    uint64_t slept = mono_us() - t0;
    CHECK(slept >= 50000 && slept < 1000000);

    interrupter_t it = { f.ep };
    pthread_t th;
    REQUIRE(pthread_create(&th, NULL, interrupt_later, &it) == 0);
    t0 = mono_us();
    CHECK(moq_media_player_wait(f.p, f.now, 5000000) == MOQ_ERR_INTERRUPTED);
    slept = mono_us() - t0;
    pthread_join(th, NULL);
    CHECK(slept >= 20000 && slept < 1000000);
    moq_endpoint_set_interrupted(f.ep, false);

    /* An error done arms the tail wait: a deadline 50 ms of player time
     * ahead, so the sleep ends there, long before the timeout. */
    CHECK(moq_pub_finish_subscribers(f.pub, pf_pub(&f, "video"), 0x4u, f.now) == MOQ_OK);
    cycles(&f, 4);
    outputs(&f, NULL);
    events(&f, NULL);
    uint64_t d = moq_media_player_next_deadline_us(f.p);
    REQUIRE(d != UINT64_MAX && d > f.now);
    t0 = mono_us();
    CHECK(moq_media_player_wait(f.p, f.now, 5000000) == MOQ_OK);
    slept = mono_us() - t0;
    CHECK(slept >= (d - f.now) / 2 && slept < 1000000);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A paused track waits on the application, but not another track's timer:
 * video's decoder is paused, audio has (0, 0) and (0, 2) with (0, 1)
 * missing. wait sleeps until audio's gap deadline and returns OK there,
 * not DONE at once; past it, tick reports the wait on (0, 1). */
static void test_paused_keeps_other_timer(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "paused_keeps_other_timer_%s", vname(ver));
    row = label;
    unsigned before = failures;
    static const char *const names[] = { "video", "audio" };
    static const char cat[] = CATALOG(LOC_VIDEO("video", "") "," LOC_AUDIO("audio", "opus", ""));
    pf_opts_t o = { .gap_timeout_us = 100000 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, cat, names, 2, &o));
    moq_endpoint_test_set_waitable(f.ep);
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    REQUIRE(select_plain(&f, "audio") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video"), *a = pf_track(&f, "audio");
    REQUIRE(v && a);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    write_loc(&f, "audio", 0, 0, true, 0, 0);
    cycles(&f, 8);
    out_log_t log = {0};
    outputs(&f, &log);
    uint64_t vgen = 0;
    for (size_t i = 0; i < log.n; i++)
        if (log.o[i].track == v) vgen = log.o[i].configuration_generation;
    pause_decoder(&f, v, vgen, 9);
    write_loc(&f, "audio", 0, 2, true, 0, 0);
    cycles(&f, 4);
    outputs(&f, NULL);
    events(&f, NULL);
    CHECK(moq_media_player_tick(f.p, f.now) >= 0);
    outputs(&f, NULL);
    events(&f, NULL);
    (void)moq_media_player_wait(f.p, f.now, 0);
    uint64_t d = moq_media_player_next_deadline_us(f.p);
    REQUIRE(d != UINT64_MAX && d > f.now);
    uint64_t t0 = mono_us();
    CHECK(moq_media_player_wait(f.p, f.now, 1000000) == MOQ_OK);
    uint64_t slept = mono_us() - t0;
    CHECK(slept >= (d - f.now) / 2 && slept < 900000);
    f.now = d;
    CHECK(moq_media_player_tick(f.p, f.now) >= 0);
    ev_log_t evl = {0};
    events(&f, &evl);
    size_t gaps = 0;
    for (size_t i = 0; i < evl.n; i++)
        gaps += evl.ev[i].kind == MOQ_MEDIA_PLAYER_EVENT_GAP && evl.ev[i].track == a &&
                !evl.ev[i].u.gap.crossed;
    CHECK(gaps == 1);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A refusal of the player's own is recorded where it happened: a
 * media-time object on a wall-clock draft-22 track is dropped TIME_DOMAIN,
 * and the engine's wait on its id reports LOCAL_REFUSAL, not an unknown
 * hole; release then resumes at the next unit. */
static void test_refusal_recorded(void)
{
    row = "refusal_recorded";
    unsigned before = failures;
    pf_opts_t o = { .gap_timeout_us = 20000 };
    pf_t f;
    REQUIRE(pf_setup(&f, MOQ_VERSION_DRAFT_22, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    write_loc(&f, "video", 0, 1, false, 90000, 0);
    write_loc(&f, "video", 0, 2, true, 0, 0);
    ev_log_t evl = {0};
    out_log_t log = {0};
    for (unsigned i = 0; i < 64; i++) {
        (void)cycle(&f);
        events(&f, &evl);
        outputs(&f, &log);
    }
    size_t refused = 0, gaps = 0;
    for (size_t i = 0; i < evl.n; i++) {
        const moq_media_player_event_t *e = &evl.ev[i];
        refused += e->kind == MOQ_MEDIA_PLAYER_EVENT_DROP &&
                   e->u.drop.reason == MOQ_MEDIA_PLAYER_DROP_TIME_DOMAIN &&
                   e->u.drop.object_id == 1;
        if (e->kind != MOQ_MEDIA_PLAYER_EVENT_GAP || e->u.gap.crossed) continue;
        gaps++;
        CHECK(e->u.gap.group_id == 0 && e->u.gap.object_id == 1);
        CHECK(e->u.gap.cause == MOQ_PLAYBACK_GAP_CAUSE_LOCAL_REFUSAL);
    }
    CHECK(refused == 1);
    CHECK(gaps == 1);
    size_t units = 0;
    for (size_t i = 0; i < log.n; i++)
        if (log.o[i].kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT) {
            units++;
            CHECK(log.o[i].u.unit.object_id != 1);
        }
    CHECK(units == 2);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A higher cause applies until END is queued, also once the track is
 * drained: a local failure drains video while a two-slot output queue keeps
 * its units and END behind; the receiver then closes (or fails) before the
 * outputs are polled, and END carries CLOSED (FATAL), IMMEDIATE. */
static void test_drained_cause_rises(moq_version_t ver, end_by_t by)
{
    static char label[64];
    snprintf(label, sizeof(label), "drained_cause_rises_%s_%s", end_by_name(by), vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .max_outputs = 2, .gap_timeout_us = UINT64_MAX };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video");
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    write_loc(&f, "video", 0, 1, false, 0, 0);
    cycles(&f, 8);
    moq_media_receiver_test_fail_track(f.r, v);
    cycles(&f, 8);
    end_by(&f, by, v, ver);
    cycles(&f, 8);
    out_log_t log = {0};
    for (unsigned i = 0; i < 24; i++) {
        (void)cycle(&f);
        outputs(&f, &log);
        events(&f, NULL);
    }
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, v) == 2);
    const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, v) == 1);
    CHECK(e->u.end.cause == (by == END_BY_FATAL ? MOQ_MEDIA_PLAYER_END_FATAL
                                                : MOQ_MEDIA_PLAYER_END_CLOSED));
    CHECK(e->u.end.disposition == MOQ_MEDIA_PLAYER_END_IMMEDIATE);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A failed track's admitted input is its own, wherever intake stopped:
 * video is paused with an object held for the engine, and audio's (0, 1)
 * to (0, 3) wait in the receiver behind that hold when audio's live
 * delivery fails. Audio does not end while they wait; once video resumes
 * they come out as units, none dropped LATE_AFTER_END, then audio's one
 * END{LOCAL_FAILURE}. */
static void test_failed_tail_behind_other_hold(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "failed_tail_behind_other_hold_%s", vname(ver));
    row = label;
    unsigned before = failures;
    static const char *const names[] = { "video", "audio" };
    static const char cat[] = CATALOG(LOC_VIDEO("video", "") "," LOC_AUDIO("audio", "opus", ""));
    pf_opts_t o = { .max_buffered_objects = 2, .gap_timeout_us = UINT64_MAX };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, cat, names, 2, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    REQUIRE(select_plain(&f, "audio") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video"), *a = pf_track(&f, "audio");
    REQUIRE(v && a);
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    write_loc(&f, "audio", 0, 0, true, 0, 0);
    cycles(&f, 8);
    out_log_t log = {0};
    outputs(&f, &log);
    uint64_t vgen = 0;
    for (size_t i = 0; i < log.n; i++)
        if (log.o[i].track == v) vgen = log.o[i].configuration_generation;
    pause_decoder(&f, v, vgen, 9);
    for (uint64_t ob = 1; ob <= 3; ob++) write_loc(&f, "video", 0, ob, false, 0, 0);
    cycles(&f, 6);
    REQUIRE(pstats(&f).held_objects == 1);
    for (uint64_t ob = 1; ob <= 3; ob++) write_loc(&f, "audio", 0, ob, true, 0, 0);
    cycles(&f, 6);
    moq_media_receiver_test_fail_delivery(f.r, a, false);
    out_log_t after = {0};
    ev_log_t evl = {0};
    for (unsigned i = 0; i < 16; i++) {
        (void)cycle(&f);
        outputs(&f, &after);
        events(&f, &evl);
    }
    CHECK(count_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_END, a) == 0);
    pause_decoder(&f, v, vgen, 0);
    for (unsigned i = 0; i < 32; i++) {
        (void)cycle(&f);
        outputs(&f, &after);
        events(&f, &evl);
    }
    CHECK(count_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, a) == 3);
    CHECK(pstats(&f).objects_dropped == 0);
    CHECK(count_ev(&evl, MOQ_MEDIA_PLAYER_EVENT_DROP) == 0);
    CHECK(count_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_END, a) == 1);
    const moq_media_player_output_t *e = last_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_END);
    REQUIRE(e != NULL);
    CHECK(e->track == a);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE);
    CHECK(count_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_END, v) == 0);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A failed track's input still in the facade is its own too: video is
 * paused with an object held, so the player takes no stream terminals and
 * video's group FINs fill the receiver's three-record terminal lane;
 * audio's (0, 1) and (0, 2) then stay in the subscriber facade when
 * audio's live delivery fails. Audio does not end while they wait. Once
 * video resumes they reach the receiver and come out as units, none
 * dropped, then audio's one END{LOCAL_FAILURE}. */
static void test_failed_tail_in_facade(moq_version_t ver)
{
    static char label[64];
    snprintf(label, sizeof(label), "failed_tail_in_facade_%s", vname(ver));
    row = label;
    unsigned before = failures;
    static const char *const names[] = { "video", "audio" };
    static const char cat[] = CATALOG(LOC_VIDEO("video", "") "," LOC_AUDIO("audio", "opus", ""));
    pf_opts_t o = { .max_buffered_objects = 2, .gap_timeout_us = UINT64_MAX,
                    .max_stream_terminals = 3 };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, cat, names, 2, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    REQUIRE(select_plain(&f, "audio") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video"), *a = pf_track(&f, "audio");
    REQUIRE(v && a);
    cycles(&f, 16);
    write_loc_eog(&f, "video", 0, 0, true, 0, 0, true);
    end_group(&f, "video");
    write_loc(&f, "audio", 0, 0, true, 0, 0);
    cycles(&f, 8);
    out_log_t log = {0};
    outputs(&f, &log);
    uint64_t vgen = 0;
    for (size_t i = 0; i < log.n; i++)
        if (log.o[i].track == v) vgen = log.o[i].configuration_generation;
    pause_decoder(&f, v, vgen, 9);
    for (uint64_t g = 1; g <= 6; g++) {
        write_loc_eog(&f, "video", g, 0, true, 0, 0, true);
        end_group(&f, "video");
        cycles(&f, 2);
    }
    REQUIRE(pstats(&f).held_objects == 1);
    write_loc(&f, "audio", 0, 1, true, 0, 0);
    write_loc(&f, "audio", 0, 2, true, 0, 0);
    cycles(&f, 6);
    moq_media_receiver_test_fail_delivery(f.r, a, false);
    out_log_t after = {0};
    ev_log_t evl = {0};
    for (unsigned i = 0; i < 16; i++) {
        (void)cycle(&f);
        outputs(&f, &after);
        events(&f, &evl);
    }
    CHECK(count_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_END, a) == 0);
    pause_decoder(&f, v, vgen, 0);
    for (unsigned i = 0; i < 60; i++) {
        (void)cycle(&f);
        outputs(&f, &after);
        events(&f, &evl);
    }
    CHECK(count_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, a) == 2);
    CHECK(count_out(&after, MOQ_MEDIA_PLAYER_OUTPUT_END, a) == 1);
    size_t late = 0;
    for (size_t k = 0; k < evl.n; k++)
        late += evl.ev[k].kind == MOQ_MEDIA_PLAYER_EVENT_DROP && evl.ev[k].track == a;
    CHECK(late == 0);
    const moq_media_player_output_t *e = NULL;
    for (size_t k = 0; k < after.n; k++)
        if (after.o[k].kind == MOQ_MEDIA_PLAYER_OUTPUT_END && after.o[k].track == a) e = &after.o[k];
    REQUIRE(e != NULL);
    CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE);
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A delivery failure names its origin: a fill child's failure leaves its
 * track playing, with no END, and later objects come out; the same failure
 * of the live delivery ends the track. */
static void test_fill_failure_keeps_parent(moq_version_t ver, bool fill)
{
    static char label[64];
    snprintf(label, sizeof(label), "delivery_failure_%s_%s", fill ? "fill" : "live",
             vname(ver));
    row = label;
    unsigned before = failures;
    pf_opts_t o = { .gap_timeout_us = UINT64_MAX };
    pf_t f;
    REQUIRE(pf_setup(&f, ver, k_cat_v, k_v, 1, &o));
    REQUIRE(select_plain(&f, "video") == MOQ_OK);
    moq_media_track_t *v = pf_track(&f, "video");
    cycles(&f, 16);
    write_loc(&f, "video", 0, 0, true, 0, 0);
    cycles(&f, 8);
    moq_media_receiver_test_fail_delivery(f.r, v, fill);
    out_log_t log = {0};
    for (unsigned i = 0; i < 8; i++) {
        (void)cycle(&f);
        outputs(&f, &log);
        events(&f, NULL);
    }
    if (fill) {
        write_loc(&f, "video", 0, 1, false, 0, 0);
        for (unsigned i = 0; i < 8; i++) {
            (void)cycle(&f);
            outputs(&f, &log);
            events(&f, NULL);
        }
    }
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, v) == (fill ? 2u : 1u));
    CHECK(count_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END, v) == (fill ? 0u : 1u));
    if (!fill) {
        const moq_media_player_output_t *e = last_out(&log, MOQ_MEDIA_PLAYER_OUTPUT_END);
        REQUIRE(e != NULL);
        CHECK(e->u.end.cause == MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE);
    }
out:
    pf_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

int main(void)
{
    const moq_version_t vers[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                   MOQ_VERSION_DRAFT_22 };
    test_contracts();
    for (size_t i = 0; i < 3; i++) {
        test_video_vertical(vers[i]);
        test_end_with_hole(vers[i]);
        test_done(vers[i], true);
        test_done(vers[i], false);
        test_receiver_closed(vers[i]);
        test_closed_with_backlog(vers[i]);
        test_held_before_facts(vers[i]);
        test_larger_than_held(vers[i]);
        test_auto_group(vers[i]);
        test_auto_ungrouped(vers[i]);
        test_unsupported(vers[i]);
        test_backpressure(vers[i]);
        test_oversize(vers[i]);
        test_feedback(vers[i]);
        test_wait(vers[i]);
        test_done_and_end(vers[i], true);
        test_done_and_end(vers[i], false);
        test_rearm_refused(vers[i]);
        test_held_done_waits(vers[i]);
        test_unrelated_pause(vers[i], true);
        test_unrelated_pause(vers[i], false);
        test_sparse_pause(vers[i], true);
        test_sparse_pause(vers[i], false);
        test_full_output_release(vers[i]);
        test_shared_capacity_pause(vers[i], false);
        test_shared_capacity_pause(vers[i], true);
        test_shared_capacity_eot(vers[i], false);
        test_shared_capacity_eot(vers[i], true);
        test_shared_capacity_sparse(vers[i], true);
        test_shared_capacity_sparse(vers[i], false);
        test_shared_capacity_failure(vers[i]);
        test_shared_capacity_error_done(vers[i]);
        test_eot_full_output(vers[i]);
        test_eot_own_pause(vers[i], true);
        test_eot_own_pause(vers[i], false);
        test_eot_bare_backlog(vers[i], true);
        test_eot_bare_backlog(vers[i], false);
        test_eot_full_queue_no_backlog(vers[i]);
        for (int how = 0; how < 4; how++) test_staged_ownership(vers[i], (staged_end_t)how);
        test_rearm_live(vers[i]);
        test_terminal_done_and_end(vers[i], true);
        test_terminal_done_and_end(vers[i], false);
        test_end_gap_then_done(vers[i]);
        test_done_then_removed(vers[i]);
        test_removed(vers[i]);
        for (int c = 0; c < 2; c++)
            for (int g = 0; g < 2; g++)
                test_complete_broadcast(vers[i], c != 0, g != 0);
        test_withdrawal_then_complete(vers[i]);
        test_audio_gap(vers[i]);
        test_deselect(vers[i]);
        test_cause_precedence(vers[i]);
        test_gap_then_complete(vers[i]);
        test_output_charge(vers[i]);
        test_paused_track_input(vers[i]);
        test_engine_backlog_is_work(vers[i]);
        for (int by = 0; by < 4; by++) {
            test_terminal_settles_held(vers[i], (end_by_t)by, false);
            test_terminal_settles_held(vers[i], (end_by_t)by, true);
        }
        test_cause_upgrades(vers[i]);
        test_gap_settled_before_end(vers[i]);
        test_gap_announced_then_filled(vers[i]);
        test_in_group_skip_and_end(vers[i], false);
        test_in_group_skip_and_end(vers[i], true);
        test_held_then_local_failure(vers[i]);
        test_local_failure(vers[i]);
        test_late_after_end(vers[i]);
        test_end_before_delayed_stream(vers[i]);
        test_wait_sleeps(vers[i]);
        test_drained_cause_rises(vers[i], END_BY_CLOSE);
        test_drained_cause_rises(vers[i], END_BY_FATAL);
        test_failed_tail_behind_other_hold(vers[i]);
        test_failed_tail_in_facade(vers[i]);
        test_fill_failure_keeps_parent(vers[i], true);
        test_fill_failure_keeps_parent(vers[i], false);
        test_paused_keeps_other_timer(vers[i]);
    }
    test_refusal_recorded();
    test_queue_bounds();
    test_replaced_configs_bounded();
    test_holds_exhausted();
    test_held_refusal();
    test_refusal_at_any_group(0);
    test_refusal_at_any_group(UINT64_MAX);
    test_player_drop_is_loss();
    test_auto_large_alternate_group();
    test_time_domain_drop();
    test_aac_config_missing();
    test_event_overflow();
    for (int v = 0; v < 3; v++) test_auto_media_time(v);
    test_cleanup_after_destroy();
    if (failures) {
        printf("FAIL: %u media player failures\n", failures);
        return 1;
    }
    printf("PASS: all media player tests\n");
    return 0;
}
