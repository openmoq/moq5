/* The receiver's MSF-02 catalog adoption, driven through the catalog intake
 * with crafted (group, object, JSON) catalog Objects and no network.
 *
 * Invariants pinned here:
 *   - an advisory change of a declared MSF-02 track keeps the handle and
 *     publishes a new description generation (TRACK_UPDATED with its
 *     number); the earlier generation stays readable and unchanged;
 *   - a decode- or delivery-critical change replaces the handle:
 *     TRACK_REMOVED naming its successor, immediately followed by the
 *     successor's TRACK_ADDED naming its predecessor; the predecessor's
 *     description is untouched;
 *   - a forbidden change drops the candidate (catalog_drops) and changes
 *     nothing; an MSF-01 catalog keeps its immutable tuple;
 *   - adoption fails whole: an allocation failure anywhere in its first
 *     phase, too little event-queue room, the handle cap or the byte cap
 *     leave the effective catalog, every handle and every selection exactly
 *     as they were, with no event queued;
 *   - the replacement moves the predecessor's latest selection to the
 *     successor, a command issued before the commit lands in it, and a
 *     command issued on the predecessor after the commit reaches the
 *     successor with the successor's result; fill commands are not carried;
 *   - the delivery policies: an encrypted track of any packaging but "loc"
 *     has no delivery path while an encrypted "loc" track keeps its path and
 *     surfaces its key fields; an in-band (track- or object-property) init
 *     is taken only for the role's property on a draft-22 companion; an
 *     MSF-02 loc audio/video track without a codec has no path; an MSF-02
 *     namespace override is subscribable and an equal namespace is no
 *     override, while an MSF-01 override has no path;
 *   - MSF_COMPRESSION: a catalog or timeline Object that carries a
 *     compression this build does not decompress is never processed as
 *     uncompressed, and the conflict, refusal and ignore counters say what
 *     happened;
 *   - SAP records take "l" or "L" but not both;
 *   - an older caller's event, stats and description sizes are never
 *     written past;
 *   - an "object"-mapped MSF-02 LOC track releases each Group's Objects in
 *     Object order: a missing Object is given up at the window and reported
 *     as a gap on the next, a RESET's cut Object at once, and a later Group
 *     waits for the current one's end (an END_OF_GROUP stream end or status
 *     Object, or the window); late and duplicate Objects are refused, and a
 *     "group"-mapped track is not ordered;
 *   - an MSF-02 CMAF fragment whose track_ID is not the init's is a media
 *     parse drop; MSF-01 delivers it.
 */
#include <moq/media_receiver.h>
#include <moq/msf.h>
#include <moq/subscriber.h>
#include <moq/vi64.h>
#include <moq/wire.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "media_relay_native_cmaf.h"

/* Test seams (media_receiver.c, MOQ_MEDIA_RECEIVER_TESTING). */
moq_media_receiver_t *moq_media_receiver_test_new_cfg(
    const moq_media_receiver_cfg_t *cfg);
void moq_media_receiver_test_free(moq_media_receiver_t *r);
void moq_media_receiver_test_set_transport_version(moq_media_receiver_t *r,
                                                   moq_version_t v);
void moq_media_receiver_test_ingest(moq_media_receiver_t *r, uint64_t group,
                                    uint64_t object, const char *json,
                                    size_t len);
uint64_t moq_media_receiver_test_catalog_drops(const moq_media_receiver_t *r);
bool moq_media_receiver_test_is_fatal(const moq_media_receiver_t *r);
uint64_t moq_media_receiver_test_fatal_code(const moq_media_receiver_t *r);
void moq_media_receiver_test_set_limits(moq_media_receiver_t *r,
                                        size_t max_stable_handles,
                                        size_t max_cp_snaps,
                                        uint64_t max_retained_bytes);
size_t moq_media_receiver_test_track_count(const moq_media_receiver_t *r);
moq_media_track_t *moq_media_receiver_test_track_at(moq_media_receiver_t *r,
                                                    size_t index);
void moq_media_receiver_test_set_receiver_alloc(moq_media_receiver_t *r,
                                                const moq_alloc_t *alloc);
bool moq_media_receiver_test_set_event_cap(moq_media_receiver_t *r, uint32_t cap);
void moq_media_receiver_test_track_intent(moq_media_receiver_t *r,
    const moq_media_track_t *t, bool *subscribed, uint32_t *filter,
    bool *has_priority, uint8_t *priority, uint32_t *ts_program,
    uint64_t *app_gen);
const moq_media_track_t *moq_media_receiver_test_track_gate(moq_media_receiver_t *r,
                                                            const moq_media_track_t *t);
int moq_media_receiver_test_track_cmd_state(moq_media_receiver_t *r,
                                            const moq_media_track_t *t);
void moq_media_receiver_test_set_adopt_hook(void (*fn)(void *, int), void *ctx);
void moq_media_receiver_test_ingest_props(moq_media_receiver_t *r, uint64_t group,
    uint64_t object, const char *json, size_t len, moq_bytes_t track_props,
    moq_bytes_t object_props);
void moq_media_receiver_test_mark_desired(moq_media_receiver_t *r,
                                          moq_media_track_t *track);
bool moq_media_receiver_test_order_start(moq_media_receiver_t *r, moq_media_track_t *t);
void moq_media_receiver_test_order_push(moq_media_receiver_t *r, moq_media_track_t *t,
    uint64_t now_us, uint64_t group, uint64_t subgroup, uint64_t object,
    moq_object_status_t status, const uint8_t *payload, size_t len);
void moq_media_receiver_test_order_terminal(moq_media_receiver_t *r, moq_media_track_t *t,
    uint64_t now_us, uint32_t kind, uint64_t group, bool has_last, uint64_t last,
    bool has_cut, uint64_t cut);
void moq_media_receiver_test_timers(moq_media_receiver_t *r, uint64_t now_us);
uint64_t moq_media_receiver_test_next_deadline_us(moq_media_receiver_t *r);
void moq_media_receiver_test_push_object_ex(moq_media_receiver_t *r, moq_media_track_t *track,
    uint64_t group, uint64_t subgroup, uint64_t object, uint8_t publisher_priority,
    moq_object_status_t status, bool end_of_group, bool datagram,
    const uint8_t *payload, size_t len, const uint8_t *properties, size_t plen);

static unsigned failures;
static const char *row = "";

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

static moq_bytes_t k_ns_parts[] = { { (const uint8_t *)"live", 4 },
                                    { (const uint8_t *)"show", 4 } };
static const moq_namespace_t k_ns = { .parts = k_ns_parts, .count = 2 };

/* A sessionless receiver of namespace live/show on `v`. */
static moq_media_receiver_t *rx_new(bool auto_subscribe, moq_version_t v,
                                    moq_media_companion_profile_t companion)
{
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = k_ns;
    cfg.auto_subscribe = auto_subscribe;
    cfg.companion_profile = companion;
    moq_media_receiver_t *r = moq_media_receiver_test_new_cfg(&cfg);
    if (r) moq_media_receiver_test_set_transport_version(r, v);
    return r;
}

static moq_media_receiver_t *rx_d18(bool auto_subscribe)
{
    return rx_new(auto_subscribe, MOQ_VERSION_DRAFT_18,
                  MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01);
}

static void ingest(moq_media_receiver_t *r, uint64_t g, uint64_t o, const char *json)
{
    moq_media_receiver_test_ingest(r, g, o, json, strlen(json));
}

typedef struct {
    moq_media_track_event_t ev[24];
    int n;
} events_t;

static int drain(moq_media_receiver_t *r, events_t *out)
{
    memset(out, 0, sizeof(*out));
    while (out->n < 24 &&
           moq_media_receiver_poll_track(r, &out->ev[out->n],
                                         sizeof(out->ev[0])) == MOQ_OK)
        out->n++;
    return out->n;
}

static bool name_is(const moq_media_track_desc_t *d, const char *s)
{
    size_t n = strlen(s);
    return d && d->name.len == n && memcmp(d->name.data, s, n) == 0;
}

static bool span_is(moq_bytes_t b, const char *s)
{
    size_t n = strlen(s);
    return b.len == n && memcmp(b.data, s, n) == 0;
}

static uint64_t drops(moq_media_receiver_t *r)
{
    return moq_media_receiver_test_catalog_drops(r);
}

/* -- Catalogs ----------------------------------------------------------- */

#define R02(tracks) "{\"version\":\"draft-02\",\"generatedAt\":1,\"tracks\":[" tracks "]}"
#define DELTA(ops) "{\"generatedAt\":2,\"deltaUpdate\":[" ops "]}"
#define VIDEO(name, codec, bitrate) \
    "{\"name\":\"" name "\",\"packaging\":\"loc\",\"isLive\":true," \
    "\"role\":\"video\",\"codec\":\"" codec "\",\"bitrate\":" #bitrate \
    ",\"framerate\":30}"

static const char k_base[] = R02(VIDEO("video", "av01", 1000) ","
                                 VIDEO("other", "av01", 500));

/* -- advisory_generation ------------------------------------------------ */

static void advisory_generation(void)
{
    row = "advisory_generation";
    events_t e;
    moq_media_receiver_t *r = rx_d18(false);
    REQUIRE(r);
    ingest(r, 0, 0, k_base);
    REQUIRE(drain(r, &e) == 3);
    REQUIRE(e.ev[0].kind == MOQ_MEDIA_TRACK_ADDED);
    moq_media_track_t *v = e.ev[0].track;
    const moq_media_track_desc_t *d0 = e.ev[0].desc;
    CHECK(e.ev[0].config_generation == 0);
    CHECK(d0 == moq_media_track_desc_get(v) && d0->config_generation == 0);
    CHECK(d0->bitrate == 1000 && name_is(d0, "video"));
    size_t handles = moq_media_receiver_test_track_count(r);

    ingest(r, 0, 1, DELTA("{\"op\":\"update\",\"tracks\":[{\"parentName\":\"video\","
                          "\"bitrate\":2000,\"label\":\"main\"}]}"));
    REQUIRE(drain(r, &e) == 1);
    CHECK(e.ev[0].kind == MOQ_MEDIA_TRACK_UPDATED);
    CHECK(e.ev[0].track == v);
    CHECK(e.ev[0].config_generation == 1);
    const moq_media_track_desc_t *d1 = e.ev[0].desc;
    REQUIRE(d1 && d1 != d0);
    CHECK(d1->config_generation == 1 && d1->bitrate == 2000 && d1->has_label &&
          span_is(d1->label, "main"));
    CHECK(moq_media_track_desc_get(v) == d1);
    /* The generation the track was discovered with is unchanged. */
    CHECK(d0->bitrate == 1000 && !d0->has_label && d0->config_generation == 0);
    CHECK(e.ev[0].replaces == NULL && e.ev[0].replaced_by == NULL);
    moq_media_track_desc_t copy;
    CHECK(moq_media_receiver_track_desc_copy(r, v, &copy, sizeof(copy)) == MOQ_OK);
    CHECK(copy.bitrate == 2000 && copy.config_generation == 1);
    CHECK(moq_media_receiver_test_track_count(r) == handles);

    /* A second advisory change is generation 2; both earlier ones stay. */
    ingest(r, 0, 2, DELTA("{\"op\":\"update\",\"tracks\":[{\"parentName\":\"video\","
                          "\"bitrate\":3000}]}"));
    REQUIRE(drain(r, &e) == 1);
    CHECK(e.ev[0].config_generation == 2 && e.ev[0].desc->bitrate == 3000);
    CHECK(d1->bitrate == 2000 && d0->bitrate == 1000);
    moq_media_receiver_stats_t st;
    CHECK(moq_media_receiver_get_stats(r, &st, sizeof(st)) == MOQ_OK);
    CHECK(st.catalog_revision == MOQ_MSF_REVISION_02);
    CHECK(st.catalog_drops == 0);
out:
    moq_media_receiver_test_free(r);
}

/* -- critical_replacement ----------------------------------------------- */

static void critical_replacement(void)
{
    row = "critical_replacement";
    events_t e;
    moq_media_receiver_t *r = rx_d18(false);
    REQUIRE(r);
    ingest(r, 0, 0, k_base);
    REQUIRE(drain(r, &e) == 3);
    moq_media_track_t *a = e.ev[0].track;
    const moq_media_track_desc_t *da = e.ev[0].desc;
    size_t handles = moq_media_receiver_test_track_count(r);

    ingest(r, 0, 1, DELTA("{\"op\":\"update\",\"tracks\":[{\"parentName\":\"video\","
                          "\"codec\":\"avc1.64001f\"}]}"));
    REQUIRE(drain(r, &e) == 2);
    CHECK(e.ev[0].kind == MOQ_MEDIA_TRACK_REMOVED && e.ev[0].track == a);
    moq_media_track_t *b = e.ev[0].replaced_by;
    REQUIRE(b && b != a);
    CHECK(e.ev[0].replaces == NULL);
    CHECK(e.ev[1].kind == MOQ_MEDIA_TRACK_ADDED && e.ev[1].track == b);
    CHECK(e.ev[1].replaces == a && e.ev[1].replaced_by == NULL);
    CHECK(e.ev[1].config_generation == 0);
    CHECK(span_is(e.ev[1].desc->codec, "avc1.64001f"));
    /* The predecessor stays valid and unchanged; it is removed. */
    CHECK(moq_media_track_desc_get(a) == da && span_is(da->codec, "av01"));
    moq_media_track_state_t s;
    CHECK(moq_media_receiver_track_state(r, a, &s) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_media_receiver_track_state(r, b, &s) == MOQ_OK &&
          s == MOQ_MEDIA_TRACK_STATE_DISCOVERED);
    CHECK(moq_media_receiver_test_track_count(r) == handles + 1);

    /* Packaging, role, codec, samplerate, channelConfig, timescale and
     * encryptionScheme are each critical. */
    static const char *const fields[] = {
        "\"samplerate\":48000", "\"channelConfig\":\"2\"", "\"timescale\":90000",
        "\"encryptionScheme\":\"moq-secure-objects\"", "\"role\":\"audio\"",
    };
    for (size_t i = 0; i < sizeof(fields) / sizeof(fields[0]); i++) {
        char json[256];
        snprintf(json, sizeof(json),
                 DELTA("{\"op\":\"update\",\"tracks\":[{\"parentName\":\"other\",%s}]}"),
                 fields[i]);
        ingest(r, 0, 2 + i, json);
        CHECK(drain(r, &e) == 2);
        CHECK(e.ev[0].kind == MOQ_MEDIA_TRACK_REMOVED && e.ev[0].replaced_by);
        CHECK(e.ev[1].kind == MOQ_MEDIA_TRACK_ADDED &&
              e.ev[1].replaces == e.ev[0].track);
    }
    CHECK(drops(r) == 0);
out:
    moq_media_receiver_test_free(r);
}

/* -- forbidden_r02 / r01_unchanged -------------------------------------- */

static void forbidden_r02(void)
{
    row = "forbidden_r02";
    events_t e;
    moq_media_receiver_t *r = rx_d18(false);
    REQUIRE(r);
    ingest(r, 0, 0, R02("{\"name\":\"vod\",\"packaging\":\"loc\",\"isLive\":false,"
                        "\"role\":\"video\",\"codec\":\"av01\",\"trackDuration\":10,"
                        "\"template\":[0,1000,[0,0],[1,0],0,1000]}"));
    REQUIRE(drain(r, &e) == 2);
    moq_media_track_t *t = e.ev[0].track;
    const moq_media_track_desc_t *d = e.ev[0].desc;
    /* isLive false -> true (5.2.8), in a later independent catalog. */
    ingest(r, 1, 0, R02("{\"name\":\"vod\",\"packaging\":\"loc\",\"isLive\":true,"
                        "\"role\":\"video\",\"codec\":\"av01\","
                        "\"template\":[0,1000,[0,0],[1,0],0,1000]}"));
    CHECK(drain(r, &e) == 0);
    CHECK(drops(r) == 1);
    /* The unchanged catalog re-syncs the stream of generations. */
    ingest(r, 2, 0, R02("{\"name\":\"vod\",\"packaging\":\"loc\",\"isLive\":false,"
                        "\"role\":\"video\",\"codec\":\"av01\",\"trackDuration\":10,"
                        "\"template\":[0,1000,[0,0],[1,0],0,1000]}"));
    CHECK(drain(r, &e) == 0);
    CHECK(drops(r) == 1);
    /* A present template changing (7.4.2). */
    ingest(r, 3, 0, R02("{\"name\":\"vod\",\"packaging\":\"loc\",\"isLive\":false,"
                        "\"role\":\"video\",\"codec\":\"av01\",\"trackDuration\":10,"
                        "\"template\":[0,2000,[0,0],[1,0],0,2000]}"));
    CHECK(drain(r, &e) == 0);
    CHECK(drops(r) == 2);
    CHECK(moq_media_track_desc_get(t) == d && !d->is_live);
    CHECK(!moq_media_receiver_test_is_fatal(r));
out:
    moq_media_receiver_test_free(r);
}

static void r01_unchanged(void)
{
    row = "r01_unchanged";
    events_t e;
    moq_media_receiver_t *r = rx_d18(false);
    REQUIRE(r);
    static const char base[] =
        "{\"version\":\"1\",\"tracks\":[{\"name\":\"video\",\"packaging\":\"loc\","
        "\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\",\"bitrate\":1000}]}";
    ingest(r, 0, 0, base);
    REQUIRE(drain(r, &e) == 2);
    moq_media_track_t *t = e.ev[0].track;
    const moq_media_track_desc_t *d = e.ev[0].desc;
    /* Any change but live-to-VOD breaks MSF-01's immutable tuple (5.3). */
    ingest(r, 1, 0, "{\"version\":\"1\",\"tracks\":[{\"name\":\"video\",\"packaging\":\"loc\","
                    "\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\",\"bitrate\":2000}]}");
    CHECK(drain(r, &e) == 0);
    CHECK(drops(r) == 1);
    CHECK(d->bitrate == 1000);
    /* Live to VOD updates the same description in place. */
    ingest(r, 2, 0, "{\"version\":\"1\",\"tracks\":[{\"name\":\"video\",\"packaging\":\"loc\","
                    "\"isLive\":false,\"trackDuration\":5000,\"role\":\"video\","
                    "\"codec\":\"av01\",\"bitrate\":1000}]}");
    REQUIRE(drain(r, &e) == 1);
    CHECK(e.ev[0].kind == MOQ_MEDIA_TRACK_UPDATED && e.ev[0].track == t);
    CHECK(e.ev[0].desc == d && e.ev[0].config_generation == 0);
    CHECK(moq_media_track_desc_get(t) == d);
    moq_media_track_desc_t copy;
    CHECK(moq_media_receiver_track_desc_copy(r, t, &copy, sizeof(copy)) == MOQ_OK);
    CHECK(!copy.is_live && copy.has_track_duration && copy.track_duration_ms == 5000);
    moq_media_receiver_stats_t st;
    CHECK(moq_media_receiver_get_stats(r, &st, sizeof(st)) == MOQ_OK);
    CHECK(st.catalog_revision == MOQ_MSF_REVISION_01);
out:
    moq_media_receiver_test_free(r);
}

/* -- preflight_failures ------------------------------------------------- */

/* An allocator that fails its n-th allocation once armed, delegating to
 * the default one. */
typedef struct {
    const moq_alloc_t *inner;
    uint64_t           armed;   /* allocations until the failure; 0 off */
    uint64_t           seen;
    bool               fired;
} fail_alloc_t;

static void *fa_alloc(size_t size, void *ctx)
{
    fail_alloc_t *f = (fail_alloc_t *)ctx;
    if (f->armed && ++f->seen == f->armed) { f->fired = true; return NULL; }
    return f->inner->alloc(size, f->inner->ctx);
}
static void *fa_realloc(void *p, size_t old_size, size_t new_size, void *ctx)
{
    fail_alloc_t *f = (fail_alloc_t *)ctx;
    if (f->armed && ++f->seen == f->armed) { f->fired = true; return NULL; }
    return f->inner->realloc(p, old_size, new_size, f->inner->ctx);
}
static void fa_free(void *p, size_t size, void *ctx)
{
    fail_alloc_t *f = (fail_alloc_t *)ctx;
    f->inner->free(p, size, f->inner->ctx);
}

/* The update every preflight row offers: an advisory change of "video", a
 * critical change of "other" and a new track. */
static const char k_mixed_update[] =
    DELTA("{\"op\":\"update\",\"tracks\":[{\"parentName\":\"video\",\"bitrate\":2000}]},"
          "{\"op\":\"update\",\"tracks\":[{\"parentName\":\"other\",\"codec\":\"vp09\"}]},"
          "{\"op\":\"add\",\"tracks\":[" VIDEO("new", "av01", 100) "]}");

/* The receiver's visible state before an update: what must survive a
 * refused one exactly. */
typedef struct {
    size_t handles;
    const moq_media_track_desc_t *desc[4];
    bool subscribed[4];
    uint64_t drops;
} snapshot_t;

static void snap(moq_media_receiver_t *r, snapshot_t *s)
{
    memset(s, 0, sizeof(*s));
    s->handles = moq_media_receiver_test_track_count(r);
    for (size_t i = 0; i < s->handles && i < 4; i++) {
        moq_media_track_t *t = moq_media_receiver_test_track_at(r, i);
        s->desc[i] = moq_media_track_desc_get(t);
        uint32_t f; bool hp; uint8_t p; uint32_t ts; uint64_t g;
        moq_media_receiver_test_track_intent(r, t, &s->subscribed[i], &f, &hp, &p, &ts, &g);
    }
    s->drops = drops(r);
}

static bool unchanged(moq_media_receiver_t *r, const snapshot_t *s)
{
    snapshot_t now;
    snap(r, &now);
    events_t e;
    bool ok = now.handles == s->handles && drain(r, &e) == 0;
    for (size_t i = 0; ok && i < s->handles && i < 4; i++)
        ok = now.desc[i] == s->desc[i] && now.subscribed[i] == s->subscribed[i];
    return ok;
}

static void preflight_failures(void)
{
    row = "preflight_failures";
    events_t e;
    moq_media_receiver_t *r = NULL;

    /* Every allocation of the update's intake fails in turn; each failure
     * changes nothing visible. The loop ends when the update succeeds. */
    bool succeeded = false;
    for (uint64_t n = 1; n < 400 && !succeeded; n++) {
        r = rx_d18(true);
        REQUIRE(r);
        fail_alloc_t fa = { moq_alloc_default(), 0, 0, false };
        moq_alloc_t alloc = { .ctx = &fa, .alloc = fa_alloc, .realloc = fa_realloc,
                              .free = fa_free };
        moq_media_receiver_test_set_receiver_alloc(r, &alloc);
        ingest(r, 0, 0, k_base);
        REQUIRE(drain(r, &e) == 3);
        snapshot_t before;
        snap(r, &before);
        fa.armed = n;
        fa.seen = 0;
        ingest(r, 0, 1, k_mixed_update);
        fa.armed = 0;
        if (!fa.fired) {
            CHECK(drain(r, &e) == 4);   /* UPDATED, REMOVED, ADDED, ADDED */
            succeeded = true;
        } else {
            bool fatal = moq_media_receiver_test_is_fatal(r);
            /* A failure while parsing or applying drops the update; one in
             * adoption is fatal, as an allocation failure always was. */
            CHECK(fatal || drops(r) == before.drops + 1);
            if (fatal)
                CHECK(moq_media_receiver_test_fatal_code(r) ==
                      MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE);
            before.drops = drops(r);
            if (!CHECK(unchanged(r, &before)))
                fprintf(stderr, "  at allocation %llu\n", (unsigned long long)n);
        }
        moq_media_receiver_test_free(r);
        r = NULL;
    }
    CHECK(succeeded);

    /* Event room one short: the update needs four events. */
    r = rx_d18(true);
    REQUIRE(r);
    REQUIRE(moq_media_receiver_test_set_event_cap(r, 5));
    ingest(r, 0, 0, k_base);
    REQUIRE(drain(r, &e) == 3);
    snapshot_t before;
    ingest(r, 0, 0, "");   /* nothing: keeps the queue empty */
    snap(r, &before);
    /* Two queued events leave three slots. */
    ingest(r, 0, 1, DELTA("{\"op\":\"update\",\"tracks\":[{\"parentName\":\"video\","
                          "\"bitrate\":1500}]}"));
    ingest(r, 0, 2, DELTA("{\"op\":\"update\",\"tracks\":[{\"parentName\":\"video\","
                          "\"bitrate\":1600}]}"));
    snap(r, &before);
    ingest(r, 0, 3, k_mixed_update);
    CHECK(drops(r) == before.drops + 1);
    CHECK(!moq_media_receiver_test_is_fatal(r));
    CHECK(drain(r, &e) == 2);   /* only the two earlier UPDATEDs */
    CHECK(moq_media_receiver_test_track_count(r) == before.handles);
    moq_media_receiver_test_free(r);

    /* The handle cap: the update adds two handles where one fits. */
    r = rx_d18(true);
    REQUIRE(r);
    ingest(r, 0, 0, k_base);
    REQUIRE(drain(r, &e) == 3);
    moq_media_receiver_test_set_limits(r, 3, 64, 16u * 1024u * 1024u);
    snap(r, &before);
    ingest(r, 0, 1, k_mixed_update);
    CHECK(drops(r) == before.drops + 1);
    before.drops = drops(r);
    CHECK(unchanged(r, &before));
    moq_media_receiver_test_free(r);

    /* The byte cap. */
    r = rx_d18(true);
    REQUIRE(r);
    ingest(r, 0, 0, k_base);
    REQUIRE(drain(r, &e) == 3);
    moq_media_receiver_test_set_limits(r, 1024, 64, 1);
    snap(r, &before);
    ingest(r, 0, 1, k_mixed_update);
    CHECK(drops(r) == before.drops + 1);
    before.drops = drops(r);
    CHECK(unchanged(r, &before));
out:
    moq_media_receiver_test_free(r);
}

/* -- Intent across a replacement ---------------------------------------- */

#define REPLACE_VIDEO(codec) \
    DELTA("{\"op\":\"update\",\"tracks\":[{\"parentName\":\"video\"," \
          "\"codec\":\"" codec "\"}]}")

static void intent_of(moq_media_receiver_t *r, const moq_media_track_t *t,
                      bool *sub, uint32_t *filter, bool *hp, uint8_t *prio,
                      uint64_t *gen)
{
    uint32_t ts;
    moq_media_receiver_test_track_intent(r, t, sub, filter, hp, prio, &ts, gen);
}

static moq_media_track_t *replaced(moq_media_receiver_t *r, moq_media_track_t *a,
                                   uint64_t obj, const char *json)
{
    events_t e;
    ingest(r, 0, obj, json);
    if (drain(r, &e) != 2 || e.ev[0].track != a || !e.ev[0].replaced_by) return NULL;
    return e.ev[0].replaced_by;
}

static void replace_subscribed_predecessor(void)
{
    row = "replace_subscribed_predecessor";
    events_t e;
    moq_media_receiver_t *r = rx_d18(false);
    REQUIRE(r);
    ingest(r, 0, 0, k_base);
    REQUIRE(drain(r, &e) == 3);
    moq_media_track_t *a = e.ev[0].track;
    moq_media_receiver_track_subscribe_cfg_t sc;
    moq_media_receiver_track_subscribe_cfg_init_sized(&sc, sizeof(sc));
    sc.start = MOQ_MEDIA_START_NEXT_GROUP;
    sc.has_priority = true;
    sc.priority = 7;
    REQUIRE(moq_media_receiver_subscribe_track(r, a, &sc) == MOQ_OK);
    moq_media_track_t *b = replaced(r, a, 1, REPLACE_VIDEO("vp09"));
    REQUIRE(b);
    bool sub, hp; uint32_t filter; uint8_t prio; uint64_t gen;
    intent_of(r, b, &sub, &filter, &hp, &prio, &gen);
    CHECK(sub && filter == MOQ_SUBSCRIBE_FILTER_NEXT_GROUP && hp && prio == 7);
    CHECK(gen == 0);
    intent_of(r, a, &sub, &filter, &hp, &prio, &gen);
    CHECK(!sub);
    /* The predecessor had no subscription: nothing gates the successor. */
    CHECK(moq_media_receiver_test_track_gate(r, b) == NULL);
out:
    moq_media_receiver_test_free(r);
}

static void replace_unsubscribed_predecessor(void)
{
    row = "replace_unsubscribed_predecessor";
    events_t e;
    /* auto_subscribe subscribes the discovered track; the application then
     * deselects it. Its replacement stays deselected: the app's intent wins
     * over the auto selection. */
    moq_media_receiver_t *r = rx_d18(true);
    REQUIRE(r);
    ingest(r, 0, 0, k_base);
    REQUIRE(drain(r, &e) == 3);
    moq_media_track_t *a = e.ev[0].track;
    REQUIRE(moq_media_receiver_unsubscribe_track(r, a) == MOQ_OK);
    moq_media_track_t *b = replaced(r, a, 1, REPLACE_VIDEO("vp09"));
    REQUIRE(b);
    bool sub, hp; uint32_t filter; uint8_t prio; uint64_t gen;
    intent_of(r, b, &sub, &filter, &hp, &prio, &gen);
    CHECK(!sub);
out:
    moq_media_receiver_test_free(r);
}

typedef struct {
    moq_media_receiver_t *r;
    moq_media_track_t    *track;
    int                   phase;      /* when to act */
    int                   action;     /* 0 unsubscribe, 1 subscribe */
    moq_result_t          rc;
    bool                  acted;
} hook_ctx_t;

static void adopt_hook(void *ctx, int phase)
{
    hook_ctx_t *h = (hook_ctx_t *)ctx;
    if (phase != h->phase || h->acted) return;
    h->acted = true;
    h->rc = h->action == 0 ? moq_media_receiver_unsubscribe_track(h->r, h->track)
                           : moq_media_receiver_subscribe_track(h->r, h->track, NULL);
}

static void command_before_commit(void)
{
    events_t e;
    for (int action = 0; action < 2; action++) {
        row = action == 0 ? "command_before_commit unsubscribe"
                          : "command_before_commit subscribe";
        moq_media_receiver_t *r = rx_d18(action == 0);
        REQUIRE(r);
        ingest(r, 0, 0, k_base);
        REQUIRE(drain(r, &e) == 3);
        moq_media_track_t *a = e.ev[0].track;
        hook_ctx_t h = { r, a, 0, action, MOQ_OK, false };
        moq_media_receiver_test_set_adopt_hook(adopt_hook, &h);
        moq_media_track_t *b = replaced(r, a, 1, REPLACE_VIDEO("vp09"));
        moq_media_receiver_test_set_adopt_hook(NULL, NULL);
        CHECK(h.acted && h.rc == MOQ_OK);
        REQUIRE(b);
        bool sub, hp; uint32_t filter; uint8_t prio; uint64_t gen;
        intent_of(r, b, &sub, &filter, &hp, &prio, &gen);
        CHECK(sub == (action == 1));
        moq_media_receiver_test_free(r);
    }
out:
    moq_media_receiver_test_set_adopt_hook(NULL, NULL);
}

static void command_after_commit(void)
{
    events_t e;
    for (int action = 0; action < 2; action++) {
        row = action == 0 ? "command_after_commit unsubscribe"
                          : "command_after_commit subscribe";
        moq_media_receiver_t *r = rx_d18(action == 0);
        REQUIRE(r);
        ingest(r, 0, 0, k_base);
        REQUIRE(drain(r, &e) == 3);
        moq_media_track_t *a = e.ev[0].track;
        hook_ctx_t h = { r, a, 1, action, MOQ_OK, false };
        moq_media_receiver_test_set_adopt_hook(adopt_hook, &h);
        ingest(r, 0, 1, REPLACE_VIDEO("vp09"));
        moq_media_receiver_test_set_adopt_hook(NULL, NULL);
        /* Issued on the predecessor after the commit, before the
         * application read the replacement events: forwarded. */
        CHECK(h.acted && h.rc == MOQ_OK);
        REQUIRE(drain(r, &e) == 2);
        moq_media_track_t *b = e.ev[0].replaced_by;
        REQUIRE(b);
        bool sub, hp; uint32_t filter; uint8_t prio; uint64_t gen;
        intent_of(r, b, &sub, &filter, &hp, &prio, &gen);
        CHECK(sub == (action == 1));
        CHECK(gen == 1);   /* the forwarded command changed B's intent */
        /* Later commands on the old handle reach B too. */
        CHECK(moq_media_receiver_subscribe_track(r, a, NULL) == MOQ_OK);
        intent_of(r, b, &sub, &filter, &hp, &prio, &gen);
        CHECK(sub);
        CHECK(moq_media_receiver_unsubscribe_track(r, a) == MOQ_OK);
        intent_of(r, b, &sub, &filter, &hp, &prio, &gen);
        CHECK(!sub);
        moq_media_receiver_test_free(r);
    }
out:
    moq_media_receiver_test_set_adopt_hook(NULL, NULL);
}

/* A chain A -> B -> C: commands on A or B after C's commit reach C, and C's
 * selection is B's latest. */
static void chain_forwarding(void)
{
    row = "chain_forwarding";
    events_t e;
    moq_media_receiver_t *r = rx_d18(false);
    REQUIRE(r);
    ingest(r, 0, 0, k_base);
    REQUIRE(drain(r, &e) == 3);
    moq_media_track_t *a = e.ev[0].track;
    moq_media_track_t *b = replaced(r, a, 1, REPLACE_VIDEO("vp09"));
    REQUIRE(b);
    CHECK(moq_media_receiver_subscribe_track(r, b, NULL) == MOQ_OK);
    moq_media_track_t *c = replaced(r, b, 2, REPLACE_VIDEO("vp08"));
    REQUIRE(c);
    bool sub, hp; uint32_t filter; uint8_t prio; uint64_t gen;
    intent_of(r, c, &sub, &filter, &hp, &prio, &gen);
    CHECK(sub);
    CHECK(moq_media_receiver_unsubscribe_track(r, a) == MOQ_OK);
    intent_of(r, c, &sub, &filter, &hp, &prio, &gen);
    CHECK(!sub);
    CHECK(moq_media_receiver_subscribe_track(r, b, NULL) == MOQ_OK);
    intent_of(r, c, &sub, &filter, &hp, &prio, &gen);
    CHECK(sub);
    intent_of(r, a, &sub, &filter, &hp, &prio, &gen);
    CHECK(!sub);
    intent_of(r, b, &sub, &filter, &hp, &prio, &gen);
    CHECK(!sub);
    /* A handle withdrawn without replacement keeps refusing. */
    ingest(r, 0, 3, DELTA("{\"op\":\"remove\",\"tracks\":[{\"name\":\"other\"}]}"));
    REQUIRE(drain(r, &e) == 1);
    CHECK(e.ev[0].kind == MOQ_MEDIA_TRACK_REMOVED && e.ev[0].replaced_by == NULL);
    CHECK(moq_media_receiver_subscribe_track(r, e.ev[0].track, NULL) ==
          MOQ_ERR_WRONG_STATE);
out:
    moq_media_receiver_test_free(r);
}

/* Fill commands belong to the predecessor's subscription generation: a
 * queued one is dropped by the replacement, and one issued afterwards on
 * the old handle goes to the successor. */
static void fill_commands_not_carried(void)
{
    row = "fill_commands_not_carried";
    events_t e;
    moq_media_receiver_t *r = rx_new(false, MOQ_VERSION_DRAFT_22,
                                     MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC);
    REQUIRE(r);
    ingest(r, 0, 0, k_base);
    REQUIRE(drain(r, &e) == 3);
    moq_media_track_t *a = e.ev[0].track;
    REQUIRE(moq_media_receiver_subscribe_track(r, a, NULL) == MOQ_OK);
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    REQUIRE(moq_media_receiver_fill_track(r, a, &fill) == MOQ_OK);
    CHECK(moq_media_receiver_test_track_cmd_state(r, a) == 1);   /* QUEUED */
    moq_media_track_t *b = replaced(r, a, 1, REPLACE_VIDEO("vp09"));
    REQUIRE(b);
    CHECK(moq_media_receiver_test_track_cmd_state(r, a) == 0);
    CHECK(moq_media_receiver_test_track_cmd_state(r, b) == 0);
    CHECK(moq_media_receiver_fill_track(r, a, &fill) == MOQ_OK);
    CHECK(moq_media_receiver_test_track_cmd_state(r, b) == 1);
    CHECK(moq_media_receiver_test_track_cmd_state(r, a) == 0);
out:
    moq_media_receiver_test_free(r);
}

/* -- Delivery policies ---------------------------------------------------- */

/* The first ADDED track of a one-catalog receiver. */
static moq_media_track_t *only_track(moq_media_receiver_t *r, const char *json)
{
    events_t e;
    ingest(r, 0, 0, json);
    if (drain(r, &e) < 1 || e.ev[0].kind != MOQ_MEDIA_TRACK_ADDED) return NULL;
    return e.ev[0].track;
}

/* The key fields of a track's description supplement, copied out while
 * the receiver (which owns them) lives. */
typedef struct {
    bool has_scheme, has_suite, has_key_id, has_base_key;
    char scheme[32], suite[32], key_id[16], base_key[16];
} enc_fields_t;

static void copy_field(char *dst, size_t cap, bool has, moq_bytes_t b)
{
    size_t n = has && b.len < cap ? b.len : 0;
    if (n) memcpy(dst, b.data, n);
    dst[n] = 0;
}

static moq_result_t subscribe_rc_enc(const char *json, moq_version_t v,
                                     moq_media_companion_profile_t companion,
                                     moq_media_track_desc_t *desc_out,
                                     enc_fields_t *enc)
{
    moq_media_receiver_t *r = rx_new(false, v, companion);
    if (!r) return MOQ_ERR_NOMEM;
    moq_media_track_t *t = only_track(r, json);
    moq_result_t rc = t ? moq_media_receiver_subscribe_track(r, t, NULL) : MOQ_ERR_INTERNAL;
    if (t && desc_out)
        (void)moq_media_receiver_track_desc_copy(r, t, desc_out, sizeof(*desc_out));
    if (t && enc) {
        const moq_media_track_desc_t *d = moq_media_track_desc_get(t);
        const moq_msf_track_supplement_t *sup =
            moq_msf_ext_supplement(d->extensions, d->extension_count);
        memset(enc, 0, sizeof(*enc));
        if (sup) {
            enc->has_scheme = sup->has_encryption_scheme;
            enc->has_suite = sup->has_cipher_suite;
            enc->has_key_id = sup->has_key_id;
            enc->has_base_key = sup->has_track_base_key;
            copy_field(enc->scheme, sizeof(enc->scheme), sup->has_encryption_scheme,
                       sup->encryption_scheme);
            copy_field(enc->suite, sizeof(enc->suite), sup->has_cipher_suite,
                       sup->cipher_suite);
            copy_field(enc->key_id, sizeof(enc->key_id), sup->has_key_id, sup->key_id);
            copy_field(enc->base_key, sizeof(enc->base_key), sup->has_track_base_key,
                       sup->track_base_key);
        }
    }
    moq_media_receiver_test_free(r);
    return rc;
}

/* desc_out's spans and arrays are owned by the freed receiver: only its
 * plain values may be read. */
static moq_result_t subscribe_rc(const char *json, moq_version_t v,
                                 moq_media_companion_profile_t companion,
                                 moq_media_track_desc_t *desc_out)
{
    return subscribe_rc_enc(json, v, companion, desc_out, NULL);
}

#define LOC_ENC(v) \
    "{\"version\":\"" v "\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\"," \
    "\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\",\"bitrate\":1," \
    "\"framerate\":30,\"encryptionScheme\":\"moq-secure-objects\"," \
    "\"cipherSuite\":\"aes-128-gcm-sha256\",\"keyId\":\"k1\"," \
    "\"trackBaseKey\":\"c2VjcmV0\"}]}"

static void encryption_boundary(void)
{
    row = "encryption_boundary";
    static const char *const revs[] = { "1", "draft-02" };
    for (size_t i = 0; i < 2; i++) {
        char json[512];
        snprintf(json, sizeof(json), LOC_ENC("%s"), revs[i]);
        enc_fields_t enc;
        /* An encrypted "loc" track keeps its path, with its key fields. */
        CHECK(subscribe_rc_enc(json, MOQ_VERSION_DRAFT_18,
                               MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01, NULL, &enc) == MOQ_OK);
        CHECK(enc.has_scheme && strcmp(enc.scheme, "moq-secure-objects") == 0);
        CHECK(enc.has_suite && strcmp(enc.suite, "aes-128-gcm-sha256") == 0);
        CHECK(enc.has_key_id && strcmp(enc.key_id, "k1") == 0);
        CHECK(enc.has_base_key && strcmp(enc.base_key, "c2VjcmV0") == 0);
    }
    /* Timelines and CMAF, which the receiver interprets, have no path when
     * encrypted; their clear twins do. */
    static const char clear_sap[] = R02("{\"name\":\"sap\",\"packaging\":\"eventtimeline\","
        "\"isLive\":true,\"eventType\":\"org.ietf.moq.cmsf.sap\","
        "\"mimeType\":\"application/json\",\"depends\":[]}");
    static const char enc_sap[] = R02("{\"name\":\"sap\",\"packaging\":\"eventtimeline\","
        "\"isLive\":true,\"eventType\":\"org.ietf.moq.cmsf.sap\","
        "\"mimeType\":\"application/json\",\"depends\":[],"
        "\"encryptionScheme\":\"moq-secure-objects\"}");
    CHECK(subscribe_rc(clear_sap, MOQ_VERSION_DRAFT_18,
                       MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01, NULL) == MOQ_OK);
    CHECK(subscribe_rc(enc_sap, MOQ_VERSION_DRAFT_18,
                       MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01, NULL) == MOQ_ERR_UNSUPPORTED);
    static const char clear_mt[] = "{\"version\":\"1\",\"tracks\":[{\"name\":\"mt\","
        "\"packaging\":\"mediatimeline\",\"isLive\":true,\"mimeType\":\"application/json\","
        "\"depends\":[\"x\"]}]}";
    static const char enc_mt[] = "{\"version\":\"1\",\"tracks\":[{\"name\":\"mt\","
        "\"packaging\":\"mediatimeline\",\"isLive\":true,\"mimeType\":\"application/json\","
        "\"depends\":[\"x\"],\"encryptionScheme\":\"moq-secure-objects\"}]}";
    CHECK(subscribe_rc(clear_mt, MOQ_VERSION_DRAFT_18,
                       MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01, NULL) == MOQ_OK);
    CHECK(subscribe_rc(enc_mt, MOQ_VERSION_DRAFT_18,
                       MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01, NULL) == MOQ_ERR_UNSUPPORTED);
}

#define INIT_CAT(type, data, role, codec) \
    "{\"version\":\"draft-02\",\"generatedAt\":1,\"initDataList\":[{\"id\":\"i\"," \
    "\"type\":\"" type "\",\"data\":\"" data "\"}],\"tracks\":[{\"name\":\"t\"," \
    "\"packaging\":\"loc\",\"isLive\":true,\"role\":\"" role "\",\"codec\":\"" codec "\"," \
    "\"bitrate\":1,\"framerate\":30,\"samplerate\":48000,\"channelConfig\":\"2\"," \
    "\"initRef\":\"i\"}]}"

static void property_init(void)
{
    row = "property_init";
    moq_media_track_desc_t d;
    const moq_media_companion_profile_t d22 = MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
    /* VIDEO_CONFIG on role video, AUDIO_CONFIG on role audio, in band. */
    memset(&d, 0, sizeof(d));
    CHECK(subscribe_rc(INIT_CAT("track-property", "0x0D", "video", "av01"),
                       MOQ_VERSION_DRAFT_22, d22, &d) == MOQ_OK);
    CHECK(d.init_source == MOQ_MEDIA_INIT_SOURCE_TRACK_PROPERTY &&
          d.init_property_type == 0x0D && d.init_data.len == 0);
    memset(&d, 0, sizeof(d));
    CHECK(subscribe_rc(INIT_CAT("object-property", "0f", "audio", "opus"),
                       MOQ_VERSION_DRAFT_22, d22, &d) == MOQ_OK);
    CHECK(d.init_source == MOQ_MEDIA_INIT_SOURCE_OBJECT_PROPERTY &&
          d.init_property_type == 0x0F);
    /* The legacy companion carries no per-Object configuration. */
    memset(&d, 0, sizeof(d));
    CHECK(subscribe_rc(INIT_CAT("track-property", "0x0D", "video", "av01"),
                       MOQ_VERSION_DRAFT_18, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                       &d) == MOQ_ERR_UNSUPPORTED);
    CHECK(d.init_source == MOQ_MEDIA_INIT_SOURCE_TRACK_PROPERTY);
    /* The property must be the role's; an unknown entry type is unusable. */
    CHECK(subscribe_rc(INIT_CAT("track-property", "0x0F", "video", "av01"),
                       MOQ_VERSION_DRAFT_22, d22, NULL) == MOQ_ERR_UNSUPPORTED);
    CHECK(subscribe_rc(INIT_CAT("future-type", "0x0D", "video", "av01"),
                       MOQ_VERSION_DRAFT_22, d22, NULL) == MOQ_ERR_UNSUPPORTED);
    /* An inline entry is INLINE; no initRef is NONE. */
    memset(&d, 0, sizeof(d));
    CHECK(subscribe_rc(INIT_CAT("inline", "AQID", "video", "av01"),
                       MOQ_VERSION_DRAFT_22, d22, &d) == MOQ_OK);
    CHECK(d.init_source == MOQ_MEDIA_INIT_SOURCE_INLINE && d.init_data.len == 3);
    memset(&d, 0, sizeof(d));
    CHECK(subscribe_rc(R02(VIDEO("v", "av01", 1)), MOQ_VERSION_DRAFT_22, d22, &d) == MOQ_OK);
    CHECK(d.init_source == MOQ_MEDIA_INIT_SOURCE_NONE && d.config_generation == 0);
}

static void required_field_policy(void)
{
    row = "required_field_policy";
    /* MSF-02 5.2.4.1: a loc audio or video track names its codec. */
    CHECK(subscribe_rc(R02("{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
                           "\"role\":\"video\",\"bitrate\":1,\"framerate\":30}"),
                       MOQ_VERSION_DRAFT_18, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                       NULL) == MOQ_ERR_UNSUPPORTED);
    /* Advisory fields missing: delivered. */
    CHECK(subscribe_rc(R02("{\"name\":\"a\",\"packaging\":\"loc\",\"isLive\":true,"
                           "\"role\":\"audio\",\"codec\":\"opus\"}"),
                       MOQ_VERSION_DRAFT_18, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                       NULL) == MOQ_OK);
}

static void namespaces(void)
{
    row = "namespaces";
    /* An MSF-02 override names its own Track Namespace: subscribable. */
    CHECK(subscribe_rc(R02("{\"name\":\"v\",\"namespace\":\"other/ns\",\"packaging\":\"loc\","
                           "\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\","
                           "\"bitrate\":1,\"framerate\":30}"),
                       MOQ_VERSION_DRAFT_18, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                       NULL) == MOQ_OK);
    /* An MSF-01 namespace string has no MoQT mapping. */
    CHECK(subscribe_rc("{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"namespace\":\"other\","
                       "\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\","
                       "\"codec\":\"av01\"}]}",
                       MOQ_VERSION_DRAFT_18, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                       NULL) == MOQ_ERR_UNSUPPORTED);
    /* A namespace equal to the receiver's is no override: the track declared
     * with it and the track declared without it are one Full Track Name. */
    events_t e;
    moq_media_receiver_t *r = rx_d18(false);
    REQUIRE(r);
    ingest(r, 0, 0, R02(VIDEO("video", "av01", 1)));
    REQUIRE(drain(r, &e) == 2);
    moq_media_track_t *t = e.ev[0].track;
    ingest(r, 1, 0, R02("{\"name\":\"video\",\"namespace\":\"live/show\",\"packaging\":\"loc\","
                        "\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\",\"bitrate\":1,"
                        "\"framerate\":30}"));
    CHECK(drain(r, &e) == 0);   /* SAME: no event, the same handle */
    CHECK(drops(r) == 0);
    CHECK(moq_media_receiver_subscribe_track(r, t, NULL) == MOQ_OK);
out:
    moq_media_receiver_test_free(r);
}

/* -- MSF_COMPRESSION ------------------------------------------------------ */

/* A property block of one integer property, encoded for `v`. */
static size_t prop_int(moq_version_t v, uint8_t *buf, uint64_t type, uint64_t value)
{
    size_t n = 0;
    if (v == MOQ_VERSION_DRAFT_16) {
        n += moq_quic_varint_encode(type, buf + n, 16 - n);
        n += moq_quic_varint_encode(value, buf + n, 16 - n);
    } else {
        n += moq_vi64_encode(type, buf + n, 16 - n);
        n += moq_vi64_encode(value, buf + n, 16 - n);
    }
    return n;
}

static moq_media_receiver_stats_t stats_of(moq_media_receiver_t *r)
{
    moq_media_receiver_stats_t st;
    memset(&st, 0, sizeof(st));
    (void)moq_media_receiver_get_stats(r, &st, sizeof(st));
    return st;
}

static void catalog_compression(void)
{
    static const moq_version_t drafts[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18 };
    for (size_t di = 0; di < 2; di++) {
        moq_version_t v = drafts[di];
        row = v == MOQ_VERSION_DRAFT_16 ? "catalog_compression d16" : "catalog_compression d18";
        uint8_t gzip[16], none[16], twice[32];
        size_t gn = prop_int(v, gzip, 0x78, 1), nn = prop_int(v, none, 0x78, 0);
        size_t tn = prop_int(v, twice, 0x78, 0);
        /* the repeated property: a zero delta repeats the type */
        tn += v == MOQ_VERSION_DRAFT_16 ? moq_quic_varint_encode(0, twice + tn, 8)
                                        : moq_vi64_encode(0, twice + tn, 8);
        tn += v == MOQ_VERSION_DRAFT_16 ? moq_quic_varint_encode(0, twice + tn, 8)
                                        : moq_vi64_encode(0, twice + tn, 8);
        moq_bytes_t g = { gzip, gn }, z = { none, nn }, two = { twice, tn }, empty = { NULL, 0 };
        events_t e;
        /* A first catalog behind GZIP leaves no usable catalog. */
        moq_media_receiver_t *r = rx_new(false, v, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01);
        REQUIRE(r);
        moq_media_receiver_test_ingest_props(r, 0, 0, k_base, strlen(k_base), empty, g);
        CHECK(moq_media_receiver_test_is_fatal(r));
        CHECK(stats_of(r).compression_refused == 1);
        moq_media_receiver_test_free(r);
        /* Value 0 on either is uncompressed. */
        r = rx_new(false, v, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01);
        REQUIRE(r);
        moq_media_receiver_test_ingest_props(r, 0, 0, k_base, strlen(k_base), z, empty);
        CHECK(drain(r, &e) == 3);
        /* The Object property where the Track property is set: dropped. */
        moq_media_receiver_test_ingest_props(r, 1, 0, k_base, strlen(k_base), z, z);
        CHECK(drain(r, &e) == 0);
        CHECK(stats_of(r).compression_conflicts == 1 && drops(r) == 1);
        /* GZIP on the Object, a repeated property, GZIP on the Track: refused,
         * never parsed as JSON. */
        moq_media_receiver_test_ingest_props(r, 2, 0, k_base, strlen(k_base), empty, g);
        moq_media_receiver_test_ingest_props(r, 3, 0, k_base, strlen(k_base), empty, two);
        moq_media_receiver_test_ingest_props(r, 4, 0, k_base, strlen(k_base), g, empty);
        CHECK(drain(r, &e) == 0);
        CHECK(stats_of(r).compression_refused == 3);
        CHECK(!moq_media_receiver_test_is_fatal(r));
        moq_media_receiver_stats_t st = stats_of(r);
        CHECK(st.objects_decompressed == 0 && st.decompress_failures == 0 &&
              st.decompress_limited == 0);
        moq_media_receiver_test_free(r);
    }
out:
    return;
}

/* -- SAP timelines ---------------------------------------------------------- */

static const char k_sap_cat02[] = R02("{\"name\":\"sap\",\"packaging\":\"eventtimeline\","
    "\"isLive\":true,\"eventType\":\"org.ietf.moq.cmsf.sap\","
    "\"mimeType\":\"application/json\",\"depends\":[]}");
static const char k_sap_cat01[] = "{\"version\":\"1\",\"tracks\":[{\"name\":\"sap\","
    "\"packaging\":\"eventtimeline\",\"isLive\":true,\"eventType\":\"org.ietf.moq.cmsf.sap\","
    "\"mimeType\":\"application/json\",\"depends\":[]}]}";

static void push_sap(moq_media_receiver_t *r, moq_media_track_t *t, uint64_t g,
                     const char *json, const uint8_t *props, size_t plen)
{
    moq_media_receiver_test_push_object_ex(r, t, g, 0, 0, 0x80, MOQ_OBJECT_NORMAL,
                                           false, false, (const uint8_t *)json,
                                           strlen(json), props, plen);
}

static int saps(moq_media_receiver_t *r)
{
    int n = 0;
    moq_media_sap_record_t rec;
    while (moq_media_receiver_poll_sap(r, &rec, sizeof(rec)) == MOQ_OK) n++;
    return n;
}

static void sap_timeline(void)
{
    row = "sap_timeline";
    moq_media_receiver_t *r = rx_d18(false);
    REQUIRE(r);
    moq_media_track_t *t = only_track(r, k_sap_cat02);
    REQUIRE(t);
    moq_media_receiver_test_mark_desired(r, t);
    /* "L" (MSF-02 8.2) and "l" (CMSF-01 3.6.1) are both read. */
    push_sap(r, t, 0, "[{\"L\":[0,0],\"data\":[1,0]}]", NULL, 0);
    push_sap(r, t, 1, "[{\"l\":[1,0],\"data\":[1,40]}]", NULL, 0);
    CHECK(saps(r) == 2);
    /* A record may use only one of them. */
    moq_media_receiver_stats_t st0 = stats_of(r);
    push_sap(r, t, 2, "[{\"l\":[2,0],\"L\":[2,0],\"data\":[1,80]}]", NULL, 0);
    push_sap(r, t, 3, "[{\"L\":[3,0],\"l\":[3,0],\"data\":[1,80]}]", NULL, 0);
    CHECK(saps(r) == 0);
    CHECK(stats_of(r).parse_drops == st0.parse_drops + 2);
    /* An Object property naming GZIP: the Object is refused, a parse drop. */
    uint8_t gzip[16];
    size_t gn = prop_int(MOQ_VERSION_DRAFT_18, gzip, 0x78, 1);
    push_sap(r, t, 4, "[{\"L\":[4,0],\"data\":[1,90]}]", gzip, gn);
    CHECK(saps(r) == 0);
    CHECK(stats_of(r).compression_refused == 1);
    moq_media_receiver_test_free(r);

    /* Under an MSF-01 catalog the property is not interpreted. */
    r = rx_d18(false);
    REQUIRE(r);
    t = only_track(r, k_sap_cat01);
    REQUIRE(t);
    moq_media_receiver_test_mark_desired(r, t);
    push_sap(r, t, 0, "[{\"l\":[0,0],\"data\":[1,0]}]", gzip, gn);
    CHECK(saps(r) == 1);
    CHECK(stats_of(r).compression_ignored == 1 && stats_of(r).compression_refused == 0);
out:
    moq_media_receiver_test_free(r);
}

/* -- Older callers' sizes ---------------------------------------------- */

static void stats_event_desc_old_size(void)
{
    row = "stats_event_desc_old_size";
    events_t e;
    moq_media_receiver_t *r = rx_d18(false);
    REQUIRE(r);
    ingest(r, 0, 0, k_base);
    REQUIRE(drain(r, &e) == 3);
    moq_media_track_t *a = e.ev[0].track;
    ingest(r, 0, 1, REPLACE_VIDEO("vp09"));
    /* The event through done_status: the replacement block is not written. */
    size_t ev_old = offsetof(moq_media_track_event_t, done_status) + sizeof(uint64_t);
    union { moq_media_track_event_t ev; unsigned char b[sizeof(moq_media_track_event_t)]; } u;
    memset(u.b, 0xA5, sizeof(u.b));
    REQUIRE(moq_media_receiver_poll_track(r, &u.ev, ev_old) == MOQ_OK);
    CHECK(u.ev.kind == MOQ_MEDIA_TRACK_REMOVED && u.ev.track == a);
    CHECK(u.ev.struct_size == ev_old);
    for (size_t i = ev_old; i < sizeof(u.b); i++) CHECK(u.b[i] == 0xA5);
    /* The full event names the successor. */
    moq_media_track_event_t full;
    REQUIRE(moq_media_receiver_poll_track(r, &full, sizeof(full)) == MOQ_OK);
    CHECK(full.kind == MOQ_MEDIA_TRACK_ADDED && full.replaces == a);
    /* The stats through tracks_malformed. */
    size_t st_old = offsetof(moq_media_receiver_stats_t, tracks_malformed) + sizeof(uint64_t);
    union { moq_media_receiver_stats_t st; unsigned char b[sizeof(moq_media_receiver_stats_t)]; } s;
    memset(s.b, 0xA5, sizeof(s.b));
    REQUIRE(moq_media_receiver_get_stats(r, &s.st, st_old) == MOQ_OK);
    CHECK(s.st.struct_size == st_old);
    for (size_t i = st_old; i < sizeof(s.b); i++) CHECK(s.b[i] == 0xA5);
    /* The description through has_label. */
    size_t d_old = offsetof(moq_media_track_desc_t, has_label) + sizeof(bool);
    union { moq_media_track_desc_t d; unsigned char b[sizeof(moq_media_track_desc_t)]; } d;
    memset(d.b, 0xA5, sizeof(d.b));
    REQUIRE(moq_media_receiver_track_desc_copy(r, full.track, &d.d, d_old) == MOQ_OK);
    CHECK(d.d.struct_size == d_old);
    for (size_t i = d_old; i < sizeof(d.b); i++) CHECK(d.b[i] == 0xA5);
out:
    moq_media_receiver_test_free(r);
}

/* -- The in-group ordering stage through the receiver ------------------- */

#define OBJECT_MODE(name) \
    "{\"name\":\"" name "\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\"," \
    "\"codec\":\"av01\",\"bitrate\":1,\"framerate\":30," \
    "\"packagingMetadata\":\"streamMapping=object\"}"
#define WINDOW_US 300000u

typedef struct { uint64_t g, o; bool gap, tail; uint64_t gg, go; bool loss; } got_t;

static int poll_objects(moq_media_receiver_t *r, got_t *out, int cap)
{
    int n = 0;
    moq_media_object_t obj;
    while (n < cap && moq_media_receiver_poll_object(r, &obj, sizeof(obj)) == MOQ_OK) {
        out[n] = (got_t){ obj.group_id, obj.object_id, obj.gap, obj.gap_tail_unknown,
                          obj.gap_group, obj.gap_object, obj.has_loss_report };
        n++;
        moq_media_object_cleanup(&obj);
    }
    return n;
}

static void push(moq_media_receiver_t *r, moq_media_track_t *t, uint64_t now,
                 uint64_t g, uint64_t o)
{
    static const uint8_t payload[] = { 0x0A, 0x0B };
    moq_media_receiver_test_order_push(r, t, now, g, o, o, MOQ_OBJECT_NORMAL,
                                       payload, sizeof(payload));
}

static moq_media_receiver_t *ordered_rx(moq_media_track_t **t)
{
    moq_media_receiver_t *r = rx_d18(false);
    if (!r) return NULL;
    *t = only_track(r, R02(OBJECT_MODE("v")));
    if (!*t) return r;
    moq_media_receiver_test_mark_desired(r, *t);
    if (!moq_media_receiver_test_order_start(r, *t)) *t = NULL;
    return r;
}

static void ordered_delivery(void)
{
    row = "ordered_delivery";
    got_t got[16];
    int n;
    moq_media_track_t *t = NULL;
    moq_media_receiver_t *r = ordered_rx(&t);
    REQUIRE(r && t);
    /* Out of order within a Group: released in Object order, loss tracked. */
    push(r, t, 1, 0, 1);
    push(r, t, 2, 0, 0);
    push(r, t, 3, 0, 2);
    REQUIRE(poll_objects(r, got, 16) == 3);
    for (int i = 0; i < 3; i++)
        CHECK(got[i].g == 0 && got[i].o == (uint64_t)i && got[i].loss && !got[i].gap);
    /* A missing Object is given up at the window: a gap on the next one. */
    push(r, t, 10, 0, 4);
    CHECK(poll_objects(r, got, 16) == 0);
    moq_media_receiver_test_timers(r, 11);   /* the pump caches the deadline */
    CHECK(moq_media_receiver_test_next_deadline_us(r) == 10 + WINDOW_US);
    moq_media_receiver_test_timers(r, 10 + WINDOW_US);
    REQUIRE(poll_objects(r, got, 16) == 1);
    CHECK(got[0].o == 4 && got[0].gap && got[0].gg == 0 && got[0].go == 3 && !got[0].tail);
    CHECK(stats_of(r).loc_order_gaps == 1);
    /* The END_OF_GROUP subgroup's FIN completes the Group: the next Group
     * goes at once, with no boundary wait. */
    moq_media_receiver_test_order_terminal(r, t, 400000, MOQ_SUB_STREAM_FIN_END_OF_GROUP,
                                           0, true, 4, false, 0);
    push(r, t, 400001, 1, 0);
    REQUIRE(poll_objects(r, got, 16) == 1);
    CHECK(got[0].g == 1 && got[0].o == 0 && !got[0].gap);
    /* A RESET that cut Object 1 short: lost at once, reported on Object 2. */
    moq_media_receiver_test_order_terminal(r, t, 400002, MOQ_SUB_STREAM_RESET,
                                           1, false, 0, true, 1);
    push(r, t, 400003, 1, 2);
    REQUIRE(poll_objects(r, got, 16) == 1);
    CHECK(got[0].o == 2 && got[0].gap && got[0].gg == 1 && got[0].go == 1);
    /* No Group end known: a later Group's Object waits up to the window
     * for Group 1's in-flight tail, which then arrives first. */
    push(r, t, 500000, 2, 0);
    CHECK(poll_objects(r, got, 16) == 0);
    push(r, t, 600000, 1, 3);
    CHECK(poll_objects(r, got, 16) == 1 && got[0].g == 1 && got[0].o == 3);
    /* The tail moved Group 1's frontier, so its wait starts again. */
    moq_media_receiver_test_timers(r, 500000 + WINDOW_US);
    CHECK(poll_objects(r, got, 16) == 0);
    moq_media_receiver_test_timers(r, 600000 + WINDOW_US);
    REQUIRE(poll_objects(r, got, 16) == 1);
    CHECK(got[0].g == 2 && got[0].o == 0 && got[0].tail);
    /* An END_OF_GROUP status Object for Group 2 avoids the wait. */
    moq_media_receiver_test_order_push(r, t, 900000, 2, 1, 1, MOQ_OBJECT_END_OF_GROUP,
                                       NULL, 0);
    push(r, t, 900001, 3, 0);
    n = poll_objects(r, got, 16);
    CHECK(n == 2 && got[0].g == 2 && got[0].o == 1 && got[1].g == 3 && got[1].o == 0);
    /* Late and duplicate Objects are refused. */
    uint64_t refused = stats_of(r).reorder_refused;
    push(r, t, 900002, 2, 0);
    push(r, t, 900003, 3, 0);
    CHECK(stats_of(r).reorder_refused == refused + 2);
out:
    moq_media_receiver_test_free(r);
}

static void ordered_end_of_track(void)
{
    row = "ordered_end_of_track";
    got_t got[16];
    moq_media_track_t *t = NULL;
    moq_media_receiver_t *r = ordered_rx(&t);
    REQUIRE(r && t);
    push(r, t, 1, 0, 0);
    push(r, t, 2, 0, 2);
    CHECK(poll_objects(r, got, 16) == 1);
    /* The subscription ends: everything held is released in order, the
     * missing Object reported, then the track ends. */
    moq_media_receiver_test_order_push(r, t, 3, 0, 3, 3, MOQ_OBJECT_END_OF_TRACK, NULL, 0);
    REQUIRE(poll_objects(r, got, 16) == 1);
    CHECK(got[0].o == 2 && got[0].gap && got[0].go == 1);
    events_t e;
    CHECK(drain(r, &e) == 1 && e.ev[0].kind == MOQ_MEDIA_TRACK_ENDED && e.ev[0].track == t);
out:
    moq_media_receiver_test_free(r);
}

/* -- CMAF fragments of another track ------------------------------------ */

/* One CMAF audio track whose inline init is the FFmpeg AAC init
 * (track_ID 1), in a catalog of `version`. */
static moq_media_track_t *cmaf_track(moq_media_receiver_t *r, const char *version)
{
    char json[2048];
    moq_rcbuf_t *b64 = NULL;
    if (moq_msf_encode_init_data(moq_alloc_default(),
            (moq_bytes_t){ k_ffmpeg_aac_stereo_init, sizeof(k_ffmpeg_aac_stereo_init) },
            &b64) != MOQ_OK)
        return NULL;
    int n = snprintf(json, sizeof(json),
        "{\"version\":\"%s\",\"generatedAt\":1,\"tracks\":[{\"name\":\"a\","
        "\"packaging\":\"cmaf\",\"isLive\":true,\"role\":\"audio\","
        "\"codec\":\"mp4a.40.2\",\"samplerate\":48000,\"channelConfig\":\"2\","
        "\"bitrate\":128000,\"timescale\":48000,\"initRef\":\"i1\"}],"
        "\"initDataList\":[{\"id\":\"i1\",\"type\":\"inline\",\"data\":\"%.*s\"}]}",
        version, (int)moq_rcbuf_len(b64), (const char *)moq_rcbuf_data(b64));
    moq_rcbuf_decref(b64);
    if (n <= 0 || (size_t)n >= sizeof(json)) return NULL;
    return only_track(r, json);
}

/* MSF-02 CMAF tracks: a fragment whose tfhd track_ID is not the init's is
 * dropped as a media parse failure. MSF-01 delivers it as before. */
static void cmaf_track_id(void)
{
    static const char *const versions[] = { "draft-02", "1" };
    for (size_t vi = 0; vi < 2; vi++) {
        row = vi == 0 ? "cmaf_track_id_r02" : "cmaf_track_id_r01";
        moq_media_receiver_t *r = rx_d18(false);
        REQUIRE(r);
        moq_media_track_t *t = cmaf_track(r, versions[vi]);
        REQUIRE(t);
        moq_media_receiver_test_mark_desired(r, t);
        uint64_t drops0 = stats_of(r).parse_drops;
        uint8_t frag[NATIVE_CMAF_FRAGMENT_SIZE];
        memcpy(frag, native_cmaf_fragment_template, sizeof(frag));
        moq_media_receiver_test_order_push(r, t, 1, 0, 0, 0, MOQ_OBJECT_NORMAL,
                                           frag, sizeof(frag));
        frag[47] = 2;   /* tfhd track_ID */
        moq_media_receiver_test_order_push(r, t, 2, 1, 0, 0, MOQ_OBJECT_NORMAL,
                                           frag, sizeof(frag));
        moq_media_object_t obj;
        int n = 0;
        bool other = false;
        while (n < 4 && moq_media_receiver_poll_object(r, &obj, sizeof(obj)) == MOQ_OK) {
            if (obj.group_id != 0) other = true;
            n++;
            moq_media_object_cleanup(&obj);
        }
        if (vi == 0) {
            CHECK(n == 1 && !other);
            CHECK(stats_of(r).parse_drops == drops0 + 1);
        } else {
            CHECK(n == 2 && other);
            CHECK(stats_of(r).parse_drops == drops0);
        }
    out:
        if (r) moq_media_receiver_test_free(r);
    }
}

static void group_mode_unchanged(void)
{
    row = "group_mode_unchanged";
    /* group mapping (the default) and MSF-01 tracks take no stage. */
    moq_media_receiver_t *r = rx_d18(false);
    REQUIRE(r);
    moq_media_track_t *t = only_track(r, R02(VIDEO("v", "av01", 1)));
    REQUIRE(t);
    CHECK(!moq_media_receiver_test_order_start(r, t));
    moq_media_receiver_test_mark_desired(r, t);
    got_t got[4];
    push(r, t, 1, 0, 1);
    REQUIRE(poll_objects(r, got, 4) == 1);
    CHECK(got[0].o == 1 && !got[0].loss);
    moq_media_receiver_test_free(r);
    r = rx_d18(false);
    REQUIRE(r);
    t = only_track(r, "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
                      "\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\","
                      "\"packagingMetadata\":\"streamMapping=object\"}]}");
    REQUIRE(t);
    CHECK(!moq_media_receiver_test_order_start(r, t));
out:
    moq_media_receiver_test_free(r);
}

int main(void)
{
    advisory_generation();
    critical_replacement();
    forbidden_r02();
    r01_unchanged();
    preflight_failures();
    replace_subscribed_predecessor();
    replace_unsubscribed_predecessor();
    command_before_commit();
    command_after_commit();
    chain_forwarding();
    fill_commands_not_carried();
    encryption_boundary();
    property_init();
    required_field_policy();
    namespaces();
    catalog_compression();
    sap_timeline();
    stats_event_desc_old_size();
    ordered_delivery();
    ordered_end_of_track();
    group_mode_unchanged();
    cmaf_track_id();
    if (failures) {
        fprintf(stderr, "%u failure(s)\n", failures);
        return 1;
    }
    printf("media_receiver_msf02: all rows passed\n");
    return 0;
}
