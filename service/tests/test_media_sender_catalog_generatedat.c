/* MSF §5.1.2 `generatedAt` must not decide catalog no-op dedup.
 *
 * `generatedAt` is "the wallclock time at which this catalog instance was
 * generated" (draft-ietf-moq-msf-01 §5.1.2), so every build stamps a fresh
 * millisecond. Production decides no-op-vs-generation by comparing the COMPLETE
 * encoded catalog against the published baseline (`sender_stage_generation`,
 * and its mirror `moq_media_sender_test_build_changed`). An add-then-remove
 * that nets back to the published tuple set is therefore deduplicated only when
 * both builds land in the same millisecond: across a tick the identical
 * catalogue is treated as a real change, advances the generation and replaces
 * retained state for nothing.
 *
 * These cases state that as a contract rather than observing it by luck. The
 * catalog clock is pinned through an instance-owned test seam, so nothing here
 * sleeps, spins, polls a boundary, or depends on machine load.
 *
 * What is asserted about catalog CONTENT is parsed back out of the emitted MSF
 * with the public parser -- never compared against bytes copied from the
 * implementation -- so "the two catalogs are semantically identical" is an
 * independent claim, not a restatement of the encoder.
 */

#include <moq/media_sender.h>
#include <moq/endpoint.h>
#include <moq/msf.h>
#include <moq/rcbuf.h>
#include <moq/sim.h>
#include <moq/session.h>
#include "test_support.h"
/* session_internal.h is included DIRECTLY rather than through
 * test_session_support.h. This file needs only the session layout, to derive
 * the track alias from pre-ingress publisher state; the broad support header
 * additionally defines white-box helpers this test never calls, one of which
 * references the private core symbol push_action(). A compiler that emits
 * unused statics -- GCC without an optimization level does -- then leaves that
 * reference in the object, and it cannot be linked against a shared
 * libmoq-core, where push_action is hidden. */
#include "../../core/src/session/session_internal.h"

static int failures = 0;

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>

/* Test seams (media_sender.c, MOQ_MEDIA_SENDER_TESTING). No header declares
 * these: they are private to the test build and add no API or ABI surface. */
moq_media_sender_t *moq_media_sender_test_new(void);
moq_media_sender_t *moq_media_sender_test_new_cfg(const moq_media_sender_cfg_t *cfg);
void moq_media_sender_test_free(moq_media_sender_t *s);
void moq_media_sender_test_set_ready(moq_media_sender_t *s);
void moq_media_sender_test_complete(moq_media_sender_t *s);
moq_result_t moq_media_sender_test_build_catalog(moq_media_sender_t *s,
                                                 moq_rcbuf_t **out);
void moq_media_sender_test_mark_published(moq_media_sender_t *s);
void moq_media_sender_test_mark_registered(moq_media_sender_t *s);
bool moq_media_sender_test_build_changed(moq_media_sender_t *s);
size_t moq_media_sender_test_stage(moq_media_sender_t *s,
                                   moq_rcbuf_t **objs, size_t cap);
uint64_t moq_media_sender_test_catalog_group(const moq_media_sender_t *s);
unsigned moq_media_sender_test_retained_installs(const moq_media_sender_t *s);
void moq_media_sender_test_pump(moq_media_sender_t *s,
                                moq_session_t *session, uint64_t now_us);
/* Backpressure at the retained-install step: the live objects of the staged
 * generation are written, the install (and therefore the commit) is deferred to
 * the next pump with the pending state intact. */
void moq_media_sender_test_block_retained_install_once(moq_media_sender_t *s);
void moq_media_sender_test_republish_live(moq_media_sender_t *s,
                                          size_t *cursor, size_t *count,
                                          unsigned *retained_installs);
/* The clock seam this file introduces: pin (or release) THIS sender's catalog
 * wallclock. Instance-owned -- a second sender in the same process keeps the
 * real clock. */
void moq_media_sender_test_set_clock_ms(moq_media_sender_t *s,
                                        bool set, uint64_t ms);

/* Declared instants. Their only requirement is that they differ; nothing here
 * waits for a real millisecond to pass. */
#define STAMP_A 1700000000123ull
#define STAMP_B 1700000000456ull
/* A third instant, same digit width as A and B: a stamp swap can never be
 * mistaken for a length change. */
#define STAMP_C 1700000000789ull

static moq_media_track_t *add_trk(moq_media_sender_t *s, const char *name)
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
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                          (int)MOQ_OK);
    return t;
}

/* The COMPLETE inventory of a catalog, derived by PARSING the emitted MSF.
 * Every root field and every track field this fixture can surface is recorded,
 * including presence bits and byte content -- so "these two catalogs differ
 * only in generatedAt" is a claim the oracle can actually make. Strings are
 * copied out of the borrowed DOM, so an inventory outlives its parse.
 *
 * The expected inventory is DECLARED by the test (see expect_baseline()), never
 * captured from a first parse. */
#define INV_MAX_TRACKS 4
#define INV_STR 40

typedef struct {
    bool     present;
    size_t   len;
    char     v[INV_STR];
    bool     truncated;      /* the fixture never uses a value this long */
} inv_str_t;

typedef struct {
    inv_str_t name, packaging, codec, namespace_, role, init_data, init_track;
    inv_str_t channel_config, lang, label, init_ref, event_type, mime_type;
    inv_str_t parent_name, parent_namespace;
    bool     is_live;
    /* Presence of the otherwise-required keys, which the clone-override model
     * makes independently observable (msf.h: has_packaging / has_is_live). */
    bool     has_packaging, has_is_live;
    /* Every optional scalar: presence AND value. Recording presence alone let a
     * changed value through. */
    bool     has_width, has_height, has_samplerate, has_framerate;
    uint32_t width, height, samplerate;
    uint64_t framerate_millis;
    bool     has_render_group, has_alt_group, has_timescale, has_target_latency;
    int      render_group, alt_group;
    uint64_t target_latency;
    bool     has_max_grp_sap, has_max_obj_sap;
    uint32_t max_grp_sap, max_obj_sap;
    bool     has_bitrate;
    uint64_t bitrate;
    uint64_t timescale;
    bool     has_track_duration;
    uint64_t track_duration_ms;
    bool     has_template;
    moq_msf_media_template_t template_;
    size_t   depends_count, cp_ref_id_count;
    bool     depends_null, cp_ref_ids_null;
} inv_track_t;

typedef struct {
    bool        parsed;
    int         version;
    bool        has_generated_at;
    uint64_t    generated_at;
    bool        is_complete;
    size_t      track_count;
    bool        tracks_null;              /* the tracks array pointer itself */
    bool        tracks_overflow;          /* more tracks than this oracle models */
    size_t      init_data_count, content_protection_count, delta_update_count;
    bool        init_data_null, content_protections_null, delta_update_null;
    inv_track_t tracks[INV_MAX_TRACKS];
} inv_t;

static void inv_str_from(inv_str_t *d, bool present, moq_bytes_t b)
{
    memset(d, 0, sizeof(*d));
    d->present = present;
    if (!present) return;
    d->len = b.len;
    if (!b.data) { d->truncated = true; return; }   /* present but no bytes */
    size_t n = b.len;
    if (n >= sizeof(d->v)) { n = sizeof(d->v) - 1; d->truncated = true; }
    memcpy(d->v, b.data, n);
    d->v[n] = '\0';
}

/* Length-delimited byte equality. Every span this fixture declares fits within
 * INV_STR, so a truncated observation is a mismatch by construction rather than
 * an equal prefix, and the stored length is bounds-checked before any read. */
static bool inv_str_eq(const inv_str_t *a, const inv_str_t *b)
{
    if (a->present != b->present) return false;
    if (!a->present) return true;
    if (a->truncated || b->truncated) return false;
    if (a->len != b->len) return false;
    if (a->len >= sizeof(a->v)) return false;
    return memcmp(a->v, b->v, a->len) == 0;
}

static void inventory(moq_rcbuf_t *j, inv_t *out)
{
    memset(out, 0, sizeof(*out));
    if (!j) return;
    const uint8_t *data = moq_rcbuf_data(j);
    size_t len = moq_rcbuf_len(j);
    if (!data || len == 0) return;
    const moq_alloc_t *al = moq_alloc_default();
    moq_msf_catalog_t c;
    if (moq_msf_catalog_parse(al, (moq_bytes_t){ data, len }, &c) != MOQ_OK)
        return;
    out->parsed = true;
    out->version = c.version;
    out->has_generated_at = c.has_generated_at;
    out->generated_at = c.generated_at;
    out->is_complete = c.is_complete;
    out->track_count = c.track_count;
    out->tracks_null = (c.tracks == NULL);
    out->tracks_overflow = c.track_count > INV_MAX_TRACKS;
    out->init_data_count = c.init_data_count;
    out->content_protection_count = c.content_protection_count;
    out->delta_update_count = c.delta_update_count;
    out->init_data_null = (c.init_data_list == NULL);
    out->content_protections_null = (c.content_protections == NULL);
    out->delta_update_null = (c.delta_update == NULL);
    for (size_t i = 0; i < c.track_count && i < INV_MAX_TRACKS; i++) {
        if (!c.tracks) break;                     /* count without array */
        const moq_msf_track_t *t = &c.tracks[i];
        inv_track_t *d = &out->tracks[i];
        inv_str_from(&d->name, true, t->name);
        inv_str_from(&d->packaging, true, t->packaging);
        inv_str_from(&d->codec, t->has_codec, t->codec);
        inv_str_from(&d->namespace_, t->has_namespace, t->namespace_);
        inv_str_from(&d->role, t->has_role, t->role);
        inv_str_from(&d->init_data, t->has_init_data, t->init_data);
        inv_str_from(&d->init_track, t->has_init_track, t->init_track);
        inv_str_from(&d->channel_config, t->has_channel_config, t->channel_config);
        inv_str_from(&d->lang, t->has_lang, t->lang);
        inv_str_from(&d->label, t->has_label, t->label);
        inv_str_from(&d->init_ref, t->has_init_ref, t->init_ref);
        inv_str_from(&d->event_type, t->has_event_type, t->event_type);
        inv_str_from(&d->mime_type, t->has_mime_type, t->mime_type);
        inv_str_from(&d->parent_name, t->has_parent_name, t->parent_name);
        inv_str_from(&d->parent_namespace, t->has_parent_namespace,
                     t->parent_namespace);
        d->is_live = t->is_live;
        d->has_packaging = t->has_packaging;
        d->has_is_live = t->has_is_live;
        d->has_width = t->has_width;
        d->width = t->has_width ? t->width : 0;
        d->has_height = t->has_height;
        d->height = t->has_height ? t->height : 0;
        d->has_samplerate = t->has_samplerate;
        d->samplerate = t->has_samplerate ? t->samplerate : 0;
        d->has_framerate = t->has_framerate;
        d->framerate_millis = t->has_framerate ? t->framerate_millis : 0;
        d->has_render_group = t->has_render_group;
        d->render_group = t->has_render_group ? t->render_group : 0;
        d->has_alt_group = t->has_alt_group;
        d->alt_group = t->has_alt_group ? t->alt_group : 0;
        d->has_timescale = t->has_timescale;
        d->timescale = t->has_timescale ? t->timescale : 0;
        d->has_target_latency = t->has_target_latency;
        d->target_latency = t->has_target_latency ? t->target_latency : 0;
        d->has_max_grp_sap = t->has_max_grp_sap;
        d->max_grp_sap = t->has_max_grp_sap ? t->max_grp_sap : 0;
        d->has_max_obj_sap = t->has_max_obj_sap;
        d->max_obj_sap = t->has_max_obj_sap ? t->max_obj_sap : 0;
        d->has_bitrate = t->has_bitrate;
        d->bitrate = t->has_bitrate ? t->bitrate : 0;
        d->has_track_duration = t->has_track_duration;
        d->track_duration_ms = t->has_track_duration ? t->track_duration_ms : 0;
        d->has_template = t->has_template;
        if (t->has_template) d->template_ = t->template_;
        d->depends_count = t->depends_count;
        d->cp_ref_id_count = t->cp_ref_id_count;
        d->depends_null = (t->depends == NULL);
        d->cp_ref_ids_null = (t->cp_ref_ids == NULL);
    }
    moq_msf_catalog_cleanup(al, &c);
}

/* Compare two inventories over EVERYTHING except generatedAt, naming the first
 * field that differs. Returns the count of differences. */
static bool g_inv_quiet = false;
static int inv_diff_except_stamp(const inv_t *a, const inv_t *b, const char *what)
{
    int d = 0;
#define D(cond, field)                                                        \
    do { if (cond) { d++;                                                      \
        if (!g_inv_quiet)                                                      \
            printf("  inventory differs (%s): %s\n", (what), (field)); } } while (0)
    D(a->parsed != b->parsed, "parsed");
    if (!a->parsed || !b->parsed) return d ? d : 1;
    D(a->version != b->version, "version");
    D(a->is_complete != b->is_complete, "isComplete");
    D(a->track_count != b->track_count, "trackCount");
    D(a->tracks_null != b->tracks_null, "tracks array presence");
    D(a->tracks_overflow != b->tracks_overflow, "tracksOverflow");
    D(a->init_data_count != b->init_data_count, "initDataCount");
    D(a->init_data_null != b->init_data_null, "initDataList presence");
    D(a->content_protection_count != b->content_protection_count, "cpCount");
    D(a->content_protections_null != b->content_protections_null, "cp presence");
    D(a->delta_update_count != b->delta_update_count, "deltaUpdateCount");
    D(a->delta_update_null != b->delta_update_null, "deltaUpdate presence");
    if (a->track_count != b->track_count) return d;
    for (size_t i = 0; i < a->track_count && i < INV_MAX_TRACKS; i++) {
        const inv_track_t *x = &a->tracks[i], *y = &b->tracks[i];
        D(!inv_str_eq(&x->name, &y->name), "track.name");
        D(!inv_str_eq(&x->packaging, &y->packaging), "track.packaging");
        D(!inv_str_eq(&x->codec, &y->codec), "track.codec");
        D(!inv_str_eq(&x->namespace_, &y->namespace_), "track.namespace");
        D(!inv_str_eq(&x->role, &y->role), "track.role");
        D(!inv_str_eq(&x->init_data, &y->init_data), "track.initData");
        D(!inv_str_eq(&x->init_track, &y->init_track), "track.initTrack");
        D(!inv_str_eq(&x->channel_config, &y->channel_config), "track.channelConfig");
        D(!inv_str_eq(&x->lang, &y->lang), "track.lang");
        D(!inv_str_eq(&x->label, &y->label), "track.label");
        D(!inv_str_eq(&x->init_ref, &y->init_ref), "track.initRef");
        D(!inv_str_eq(&x->event_type, &y->event_type), "track.eventType");
        D(!inv_str_eq(&x->mime_type, &y->mime_type), "track.mimeType");
        D(!inv_str_eq(&x->parent_name, &y->parent_name), "track.parentName");
        D(!inv_str_eq(&x->parent_namespace, &y->parent_namespace),
          "track.parentNamespace");
        D(x->is_live != y->is_live, "track.isLive");
        D(x->has_packaging != y->has_packaging, "track.packaging presence bit");
        D(x->has_is_live != y->has_is_live, "track.isLive presence bit");
        D(x->has_bitrate != y->has_bitrate || x->bitrate != y->bitrate, "track.bitrate");
        D(x->has_width != y->has_width || x->width != y->width, "track.width");
        D(x->has_height != y->has_height || x->height != y->height, "track.height");
        D(x->has_samplerate != y->has_samplerate ||
          x->samplerate != y->samplerate, "track.samplerate");
        D(x->has_framerate != y->has_framerate ||
          x->framerate_millis != y->framerate_millis, "track.framerate");
        D(x->has_render_group != y->has_render_group ||
          x->render_group != y->render_group, "track.renderGroup");
        D(x->has_alt_group != y->has_alt_group ||
          x->alt_group != y->alt_group, "track.altGroup");
        D(x->has_timescale != y->has_timescale ||
          x->timescale != y->timescale, "track.timescale");
        D(x->has_target_latency != y->has_target_latency ||
          x->target_latency != y->target_latency, "track.targetLatency");
        D(x->has_max_grp_sap != y->has_max_grp_sap ||
          x->max_grp_sap != y->max_grp_sap, "track.maxGrpSap");
        D(x->has_max_obj_sap != y->has_max_obj_sap ||
          x->max_obj_sap != y->max_obj_sap, "track.maxObjSap");
        D(x->has_track_duration != y->has_track_duration ||
          x->track_duration_ms != y->track_duration_ms, "track.trackDuration");
        D(x->has_template != y->has_template, "track.template presence");
        if (x->has_template && y->has_template)
            D(memcmp(&x->template_, &y->template_, sizeof(x->template_)) != 0,
              "track.template value");
        D(x->depends_count != y->depends_count || x->depends_null != y->depends_null,
          "track.depends");
        D(x->cp_ref_id_count != y->cp_ref_id_count ||
          x->cp_ref_ids_null != y->cp_ref_ids_null, "track.cpRefIDs");
    }
#undef D
    return d;
}

/* Build the catalog and RETAIN the encoded buffer, so the byte oracle and the
 * inventory look at the same artifact. Caller decrefs. */
static moq_rcbuf_t *build_retained(moq_media_sender_t *s, inv_t *out)
{
    moq_rcbuf_t *j = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &j),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(j != NULL);
    inventory(j, out);
    return j;
}

static void build_inventory(moq_media_sender_t *s, inv_t *out)
{
    moq_rcbuf_t *j = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &j),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(j != NULL);
    inventory(j, out);
    if (j) moq_rcbuf_decref(j);
}

static bool rcbuf_eq(moq_rcbuf_t *a, moq_rcbuf_t *b)
{
    if (!a || !b) return false;
    const uint8_t *pa = moq_rcbuf_data(a), *pb = moq_rcbuf_data(b);
    size_t la = moq_rcbuf_len(a), lb = moq_rcbuf_len(b);
    if (!pa || !pb) return false;
    if (la != lb) return false;
    return memcmp(pa, pb, la) == 0;
}

/* The DECLARED expectation for a live video track this fixture adds. Written
 * from the fixture's own inputs -- never copied out of a parsed catalog. */
static void expect_track(inv_track_t *d, const char *name)
{
    memset(d, 0, sizeof(*d));
    d->name.present = true;  d->name.len = strlen(name);
    snprintf(d->name.v, sizeof(d->name.v), "%s", name);
    /* MOQ_MEDIA_PACKAGING_RAW */
    d->packaging.present = true; d->packaging.len = 3;
    snprintf(d->packaging.v, sizeof(d->packaging.v), "loc");
    d->codec.present = true; d->codec.len = 4;
    snprintf(d->codec.v, sizeof(d->codec.v), "av01");
    d->is_live = true;
    d->has_bitrate = true; d->bitrate = 1500000;
    /* MOQ_MEDIA_TYPE_VIDEO with no explicit role resolves to the default role
     * "video" (media_sender.c default_role); mimeType is NOT emitted for a
     * plain media track -- only the generated media-timeline and SAP-timeline
     * tracks carry application/json. */
    d->role.present = true; d->role.len = 5;
    snprintf(d->role.v, sizeof(d->role.v), "video");
    /* add_track defaults an absent timescale to 1000000. */
    d->has_timescale = true; d->timescale = 1000000u;
    /* An independent (non-clone) track always carries packaging and isLive, so
     * both presence bits are set; parentName/parentNamespace, trackDuration and
     * template are clone/VOD/timeline-only and must be ABSENT here
     * (trackDuration MUST NOT appear on a live track at all). */
    d->has_packaging = true;
    d->has_is_live = true;
    d->depends_null = true;
    d->cp_ref_ids_null = true;
    /* Everything else is deliberately ABSENT: the fixture sets no dimensions,
     * no rates, no grouping, no language/label, no init data, no protection,
     * and no mimeType. */
}

static void expect_baseline(inv_t *e, uint64_t stamp, const char *const *names,
                            size_t n)
{
    memset(e, 0, sizeof(*e));
    e->parsed = true;
    e->version = MOQ_MSF_VERSION;
    e->has_generated_at = true;
    e->generated_at = stamp;
    e->is_complete = false;
    e->track_count = n;
    /* An empty track list allocates no array: the terminal (complete) catalog
     * carries a zero count AND a null pointer, and a live one carries both. */
    e->tracks_null = (n == 0);
    e->init_data_null = true;
    e->content_protections_null = true;
    e->delta_update_null = true;
    for (size_t i = 0; i < n && i < INV_MAX_TRACKS; i++)
        expect_track(&e->tracks[i], names[i]);
}

/* Assert an inventory against a declared expectation, INCLUDING generatedAt. */
static void assert_inventory(const inv_t *got, const inv_t *want, const char *what)
{
    MOQ_TEST_CHECK(got->parsed);
    MOQ_TEST_CHECK_EQ_U64(got->generated_at, want->generated_at);
    MOQ_TEST_CHECK(got->has_generated_at == want->has_generated_at);
    int d = inv_diff_except_stamp(got, want, what);
    MOQ_TEST_CHECK_EQ_INT(d, 0);
}

/* -- 1..4: the generatedAt dedup contract --------------------------------
 *
 * `equal_stamps` is the anti-vacuity perturbation: with both builds pinned to
 * the SAME instant the declared inventory is unchanged, the two catalogs are
 * byte-IDENTICAL, and the no-op dedup succeeds -- which is what shows the
 * failure below is attributable to `generatedAt` and to nothing else.
 *
 * The two instants have the same number of digits, so the encoded catalogs are
 * the same LENGTH when they differ: the byte oracle is a content
 * discriminator, not a length one, and that is asserted rather than assumed. */
static void test_noop_dedup(bool equal_stamps)
{
    moq_media_sender_t *s = moq_media_sender_test_new();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return;
    moq_media_sender_test_set_clock_ms(s, true, STAMP_A);
    add_trk(s, "v");
    add_trk(s, "a");
    /* Staging builds the ONLY-REGISTERED view of the catalog, so the tracks are
     * marked registered here: otherwise the staged and published catalogs would
     * differ over the tuple set and the dedup question would never be reached. */
    moq_media_sender_test_mark_registered(s);
    moq_media_sender_test_set_ready(s);

    /* 1. Baseline committed at A, checked against a DECLARED inventory. */
    static const char *const names2[] = { "v", "a" };
    inv_t want;
    expect_baseline(&want, STAMP_A, names2, 2);
    inv_t base;
    moq_rcbuf_t *base_buf = build_retained(s, &base);
    assert_inventory(&base, &want, "baseline");
    moq_media_sender_test_mark_published(s);
    uint64_t g0 = moq_media_sender_test_catalog_group(s);

    /* 2. Add then remove the same track: the resolved tuple set is the
     *    published one again. Then move the clock to B (or keep it at A). */
    moq_media_track_t *x = add_trk(s, "x");
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, x),
                          (int)MOQ_OK);
    uint64_t stamp2 = equal_stamps ? STAMP_A : STAMP_B;
    moq_media_sender_test_set_clock_ms(s, true, stamp2);

    /* 3. The same declared inventory except the timestamp, and the encoded
     *    bytes are equal exactly when the timestamps are. */
    inv_t want2;
    expect_baseline(&want2, stamp2, names2, 2);
    inv_t now;
    moq_rcbuf_t *now_buf = build_retained(s, &now);
    assert_inventory(&now, &want2, "after add+remove");
    MOQ_TEST_CHECK_EQ_INT(inv_diff_except_stamp(&base, &now, "base vs now"), 0);
    if (base_buf && now_buf) {
        /* Same length either way: only the digits of the stamp change. */
        MOQ_TEST_CHECK_EQ_U64((uint64_t)moq_rcbuf_len(base_buf),
                              (uint64_t)moq_rcbuf_len(now_buf));
        MOQ_TEST_CHECK(rcbuf_eq(base_buf, now_buf) == equal_stamps);
    }
    if (base_buf) moq_rcbuf_decref(base_buf);
    if (now_buf) moq_rcbuf_decref(now_buf);

    /* 4. The contract: a catalog that nets back to the published one is a
     *    no-op, whichever millisecond it was generated in. */
    MOQ_TEST_CHECK(!moq_media_sender_test_build_changed(s));

    moq_rcbuf_t *objs[8] = {0};
    size_t n = moq_media_sender_test_stage(s, objs, 8);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)n, 0u);        /* nothing staged */
    for (size_t i = 0; i < n && i < 8; i++)
        if (objs[i]) moq_rcbuf_decref(objs[i]);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), g0);

    moq_media_sender_test_free(s);
}

/* -- 5: a genuine add is still a change --------------------------------- */
static void test_real_add_still_changes(void)
{
    moq_media_sender_t *s = moq_media_sender_test_new();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return;
    moq_media_sender_test_set_clock_ms(s, true, STAMP_A);
    add_trk(s, "v");
    moq_media_sender_test_mark_registered(s);
    moq_media_sender_test_set_ready(s);
    static const char *const one[] = { "v" };
    static const char *const two[] = { "v", "y" };
    inv_t want1;
    expect_baseline(&want1, STAMP_A, one, 1);
    inv_t base;
    moq_rcbuf_t *base_buf = build_retained(s, &base);
    assert_inventory(&base, &want1, "one-track baseline");
    moq_media_sender_test_mark_published(s);

    /* Same instant as the baseline: the difference is the tuple set alone.
     * The new track is registered too, so it is present in the only-registered
     * view staging builds -- otherwise this would test the registration
     * filter rather than the change decision. */
    add_trk(s, "y");
    moq_media_sender_test_mark_registered(s);
    inv_t want2;
    expect_baseline(&want2, STAMP_A, two, 2);
    inv_t now;
    moq_rcbuf_t *now_buf = build_retained(s, &now);
    assert_inventory(&now, &want2, "two-track catalog");
    /* Genuinely different content, at an identical timestamp. The diff is
     * EXPECTED here, so its per-field narration is silenced -- otherwise this
     * case would print lines that read like failures. */
    g_inv_quiet = true;
    int adds = inv_diff_except_stamp(&base, &now, "add");
    g_inv_quiet = false;
    MOQ_TEST_CHECK(adds > 0);
    MOQ_TEST_CHECK_EQ_U64(now.generated_at, base.generated_at);
    MOQ_TEST_CHECK(!rcbuf_eq(base_buf, now_buf));
    if (base_buf) moq_rcbuf_decref(base_buf);
    if (now_buf) moq_rcbuf_decref(now_buf);
    MOQ_TEST_CHECK(moq_media_sender_test_build_changed(s));

    moq_rcbuf_t *objs[8] = {0};
    size_t n = moq_media_sender_test_stage(s, objs, 8);
    MOQ_TEST_CHECK(n > 0);
    for (size_t i = 0; i < n && i < 8; i++)
        if (objs[i]) moq_rcbuf_decref(objs[i]);

    moq_media_sender_test_free(s);
    MOQ_TEST_PASS("generatedat.real_add_still_changes");
}

/* -- 7: completion is a real terminal generation ------------------------
 *
 * The discriminating shape is an EMPTY published catalog completing to an EMPTY
 * terminal catalog: the tuple set does not change at all, the clock does not
 * move, and the ONLY difference is `isComplete`. A correction that decided
 * no-op from the tuple diff alone would suppress exactly this, so it is the arm
 * that protects completion from the planned change.
 *
 * `empty_start` false keeps the weaker {v} -> {} shape as well, so a regression
 * that broke only the populated case is still visible. */
static void test_completion_still_generates(bool empty_start)
{
    moq_media_sender_t *s = moq_media_sender_test_new();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return;
    moq_media_sender_test_set_clock_ms(s, true, STAMP_A);
    if (!empty_start) add_trk(s, "v");
    moq_media_sender_test_mark_registered(s);
    moq_media_sender_test_set_ready(s);

    /* The published baseline: for the empty arm this is already a zero-track
     * catalog, so completion changes no tuple. */
    inv_t pub;
    moq_rcbuf_t *pub_buf = build_retained(s, &pub);
    MOQ_TEST_CHECK(pub.parsed);
    MOQ_TEST_CHECK(!pub.is_complete);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)pub.track_count, empty_start ? 0u : 1u);
    if (pub_buf) moq_rcbuf_decref(pub_buf);
    moq_media_sender_test_mark_published(s);
    uint64_t g0 = moq_media_sender_test_catalog_group(s);

    /* Complete at the SAME instant: if completion still cuts a generation
     * here, it cannot be relying on a timestamp difference to do it. */
    moq_media_sender_test_complete(s);
    inv_t want;
    expect_baseline(&want, STAMP_A, NULL, 0);
    want.is_complete = true;
    inv_t term;
    moq_rcbuf_t *term_buf = build_retained(s, &term);
    assert_inventory(&term, &want, "terminal catalog");
    if (term_buf) moq_rcbuf_decref(term_buf);
    MOQ_TEST_CHECK(moq_media_sender_test_build_changed(s));

    moq_rcbuf_t *objs[8] = {0};
    size_t n = moq_media_sender_test_stage(s, objs, 8);
    MOQ_TEST_CHECK(n > 0);
    if (n > 0 && n <= 8 && objs[0]) {
        inv_t staged;
        inventory(objs[0], &staged);
        assert_inventory(&staged, &want, "staged terminal object");
    }
    for (size_t i = 0; i < n && i < 8; i++)
        if (objs[i]) moq_rcbuf_decref(objs[i]);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), g0);

    moq_media_sender_test_free(s);
}

/* -- instance isolation: the seam is per-sender ------------------------- */
static void test_clock_seam_is_instance_owned(void)
{
    moq_media_sender_t *a = moq_media_sender_test_new();
    moq_media_sender_t *b = moq_media_sender_test_new();
    MOQ_TEST_CHECK(a != NULL && b != NULL);
    if (!a || !b) {
        if (a) moq_media_sender_test_free(a);
        if (b) moq_media_sender_test_free(b);
        return;
    }
    moq_media_sender_test_set_clock_ms(a, true, STAMP_A);
    add_trk(a, "v"); add_trk(b, "v");
    moq_media_sender_test_set_ready(a);
    moq_media_sender_test_set_ready(b);

    inv_t ia, ib;
    build_inventory(a, &ia);
    build_inventory(b, &ib);
    MOQ_TEST_CHECK(ia.parsed && ib.parsed);
    MOQ_TEST_CHECK_EQ_U64(ia.generated_at, STAMP_A);
    /* b never pinned: it carries the real wallclock, which is not the
     * declared constant. */
    MOQ_TEST_CHECK(ib.has_generated_at);
    MOQ_TEST_CHECK(ib.generated_at != STAMP_A);

    /* Releasing the pin restores the real clock for a as well. */
    moq_media_sender_test_set_clock_ms(a, false, 0);
    inv_t ia2;
    build_inventory(a, &ia2);
    MOQ_TEST_CHECK(ia2.parsed);
    MOQ_TEST_CHECK(ia2.generated_at != STAMP_A);

    moq_media_sender_test_free(a);
    moq_media_sender_test_free(b);
    MOQ_TEST_PASS("generatedat.clock_seam_instance_owned");
}

/* -- 6: the intentional periodic refresh is NOT dedup ------------------- *
 *
 * The automatic refresh exists to put a fresh independent catalog on the
 * subscribe path for late joiners, so it must cut a real generation even
 * though its tuple set is unchanged -- exactly the case the no-op rule above
 * suppresses -- the semantic comparison must not be allowed to swallow it.
 *
 * The OBJECT the refresh actually delivers is what is inspected, over a real
 * SimPair subscribe: a locally rebuilt catalog would prove nothing about what
 * the peer received.
 *
 * The peer stream is classified BY PHASE, and every phase is exact. Setup
 * observations (SETUP_COMPLETE, NAMESPACE_PUBLISHED, SUBSCRIBE_OK) are claimed
 * by a declared inventory in the phase that legitimately produces them, and are
 * required to be fully consumed before the refresh window opens; inside that
 * window they are unexpected, because a late or duplicate control event there
 * is exactly the kind of traffic a quiet drain would hide.
 *
 * Only the SUBSCRIBER session is classified after the sender exists: from that
 * point the sender owns its own session's event stream (moq_media_sender_pump
 * drains it), so the fixture has no claim on it. The client side is classified
 * in the setup phase, before the sender is created.
 */
static moq_bytes_t g_ns_parts[2];

/* Exactly one SETUP_COMPLETE with the declared perspectives and an empty token
 * inventory, and no other event, on one side. */
static void expect_setup_only(moq_session_t *sess, const char *who,
                              int local, int peer)
{
    moq_event_t ev;
    int setups = 0, others = 0;
    uint64_t other_kind = 0;
    int got_local = 0, got_peer = 0;
    size_t tok_n = 0;
    bool tok_null = true;
    while (moq_session_poll_events(sess, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) {
            setups++;
            got_local = (int)ev.u.setup_complete.local_perspective;
            got_peer  = (int)ev.u.setup_complete.peer_perspective;
            tok_n     = ev.u.setup_complete.token_count;
            tok_null  = (ev.u.setup_complete.tokens == NULL);
        } else {
            others++;
            other_kind = (uint64_t)ev.kind;
        }
        moq_event_cleanup(&ev);
    }
    if (setups != 1 || others != 0)
        printf("  setup phase (%s): %d SETUP_COMPLETE, %d other (last kind %llu)\n",
               who, setups, others, (unsigned long long)other_kind);
    MOQ_TEST_CHECK_EQ_INT(setups, 1);
    MOQ_TEST_CHECK_EQ_INT(others, 0);
    MOQ_TEST_CHECK_EQ_INT(got_local, local);
    MOQ_TEST_CHECK_EQ_INT(got_peer, peer);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)tok_n, 0u);
    MOQ_TEST_CHECK(tok_null);
}

/* Everything the setup window on the subscriber side may legitimately produce,
 * recorded in full at poll time and compared afterwards against values declared
 * from the fixture's own inputs and from pre-ingress source state. */
typedef struct {
    int      ns_pub, sub_ok, other;
    uint64_t other_kind;
    /* NAMESPACE_PUBLISHED body */
    bool     ann_valid;
    size_t   ns_count;
    bool     ns_parts_match;
    size_t   ns_tokens;
    bool     ns_tokens_null;
    int      accept_rc;
    /* SUBSCRIBE_OK body */
    uint64_t sok_sub, sok_alias;
    bool     sok_has_largest;
    uint64_t sok_lg, sok_lo;
    bool     sok_has_expires;
    uint64_t sok_expires_ms;
    size_t   sok_props_len;
    bool     sok_dynamic_groups;
} setup_obs_t;

/* Classify one poll pass of the subscriber's setup window. Accepting the
 * announcement is part of the phase, so it happens here, and its result is
 * recorded rather than discarded. */
static void classify_setup(moq_session_t *srv, setup_obs_t *o, uint64_t now_us)
{
    moq_event_t ev;
    while (moq_session_poll_events(srv, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            const moq_namespace_published_event_t *n = &ev.u.namespace_published;
            o->ns_pub++;
            o->ann_valid = (n->ann._opaque != 0);
            o->ns_count = n->track_namespace.count;
            o->ns_tokens = n->token_count;
            o->ns_tokens_null = (n->tokens == NULL);
            o->ns_parts_match = false;
            if (n->track_namespace.parts && n->track_namespace.count == 2) {
                const moq_bytes_t *p = n->track_namespace.parts;
                o->ns_parts_match =
                    p[0].data && p[1].data &&
                    p[0].len == g_ns_parts[0].len &&
                    p[1].len == g_ns_parts[1].len &&
                    memcmp(p[0].data, g_ns_parts[0].data, p[0].len) == 0 &&
                    memcmp(p[1].data, g_ns_parts[1].data, p[1].len) == 0;
            }
            moq_accept_namespace_cfg_t ac;
            moq_accept_namespace_cfg_init(&ac);
            o->accept_rc = (int)moq_session_accept_namespace(srv, n->ann, &ac,
                                                             now_us);
        } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) {
            const moq_subscribe_ok_event_t *k = &ev.u.subscribe_ok;
            o->sub_ok++;
            o->sok_sub = k->sub._opaque;
            o->sok_alias = k->track_alias;
            o->sok_has_largest = k->has_largest;
            o->sok_lg = k->largest_group;
            o->sok_lo = k->largest_object;
            o->sok_has_expires = k->has_expires;
            o->sok_expires_ms = k->expires_ms;
            o->sok_props_len = k->track_properties.len;
            o->sok_dynamic_groups = k->dynamic_groups;
        } else {
            o->other++;
            o->other_kind = (uint64_t)ev.kind;
        }
        moq_event_cleanup(&ev);
    }
}

/* One delivered object, recorded in FULL at poll time. Identity is compared
 * before any payload is used, so a wrong subscription or wrong metadata cannot
 * satisfy the payload oracle by carrying the right bytes. */
#define PEER_OBJ_MAX 4
typedef struct {
    uint64_t sub_opaque, pub_opaque;
    uint64_t group_id, subgroup_id, object_id;
    int      publisher_priority;
    int      status;
    bool     end_of_group, datagram;
    bool     have_payload, have_properties;
    size_t   payload_len;
    inv_t    body;            /* parsed payload of THIS object */
} obj_rec_t;

typedef struct {
    int      objects;              /* OBJECT_RECEIVED seen */
    bool     obj_overflow;         /* more objects than this oracle records */
    int      unexpected;           /* any other event kind -- including setup */
    uint64_t last_unexpected_kind;
    obj_rec_t obj[PEER_OBJ_MAX];
} peer_stream_t;

static void classify_peer(moq_session_t *peer, peer_stream_t *ps)
{
    moq_event_t ev;
    while (moq_session_poll_events(peer, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            const moq_object_received_event_t *o = &ev.u.object_received;
            ps->objects++;
            if (ps->objects <= PEER_OBJ_MAX) {
                obj_rec_t *r = &ps->obj[ps->objects - 1];
                r->sub_opaque = o->sub._opaque;
                r->pub_opaque = o->pub._opaque;
                r->group_id = o->group_id;
                r->subgroup_id = o->subgroup_id;
                r->object_id = o->object_id;
                r->publisher_priority = (int)o->publisher_priority;
                r->status = (int)o->status;
                r->end_of_group = o->end_of_group;
                r->datagram = o->datagram;
                r->have_payload = (o->payload != NULL);
                r->have_properties = (o->properties != NULL);
                r->payload_len = o->payload ? moq_rcbuf_len(o->payload) : 0;
                if (o->payload) inventory(o->payload, &r->body);
            } else {
                ps->obj_overflow = true;
            }
        } else {
            ps->unexpected++;
            ps->last_unexpected_kind = (uint64_t)ev.kind;
        }
        moq_event_cleanup(&ev);
    }
}

/* The complete declared identity of the delivered catalog object. */
typedef struct {
    uint64_t sub_opaque, pub_opaque;
    uint64_t group_id, subgroup_id, object_id;
    int      publisher_priority;
    int      status;
    bool     end_of_group, datagram;
    bool     have_payload, have_properties;
} obj_want_t;

static void assert_object_identity(const obj_rec_t *g, const obj_want_t *w,
                                   const char *what)
{
#define OD(cond, field)                                                       \
    do { if (cond) printf("  object differs (%s): %s\n", (what), (field)); } while (0)
    OD(g->sub_opaque != w->sub_opaque, "sub");
    OD(g->pub_opaque != w->pub_opaque, "pub");
    OD(g->group_id != w->group_id, "groupId");
    OD(g->subgroup_id != w->subgroup_id, "subgroupId");
    OD(g->object_id != w->object_id, "objectId");
    OD(g->publisher_priority != w->publisher_priority, "publisherPriority");
    OD(g->status != w->status, "status");
    OD(g->end_of_group != w->end_of_group, "endOfGroup");
    OD(g->datagram != w->datagram, "datagram");
    OD(g->have_payload != w->have_payload, "payload presence");
    OD(g->have_properties != w->have_properties, "properties presence");
#undef OD
    MOQ_TEST_CHECK_EQ_U64(g->sub_opaque, w->sub_opaque);
    MOQ_TEST_CHECK_EQ_U64(g->pub_opaque, w->pub_opaque);
    MOQ_TEST_CHECK_EQ_U64(g->group_id, w->group_id);
    MOQ_TEST_CHECK_EQ_U64(g->subgroup_id, w->subgroup_id);
    MOQ_TEST_CHECK_EQ_U64(g->object_id, w->object_id);
    MOQ_TEST_CHECK_EQ_INT(g->publisher_priority, w->publisher_priority);
    MOQ_TEST_CHECK_EQ_INT(g->status, w->status);
    MOQ_TEST_CHECK(g->end_of_group == w->end_of_group);
    MOQ_TEST_CHECK(g->datagram == w->datagram);
    MOQ_TEST_CHECK(g->have_payload == w->have_payload);
    MOQ_TEST_CHECK(g->have_properties == w->have_properties);
}

/* Everything both SimPair arms need, established ONCE. Factoring the setup is
 * not tidiness: the two arms must consume the SAME complete phase assertions, so
 * a check present in one and missing in the other cannot exist. Expectations
 * stay derived from the fixture's own inputs and from pre-ingress source state,
 * never from the events being classified. */
typedef struct {
    moq_simpair_t     *sp;
    moq_session_t     *cl, *srv;
    moq_media_sender_t *s;
    uint64_t           t0;
    moq_subscription_t sub;
    uint64_t           want_alias;
} gen_fixture_t;

/* Phase 1's complete declared inventory: exactly one announcement, this
 * fixture's own two namespace parts, an empty token inventory, accepted -- and
 * nothing else, from any phase. */
static void assert_namespace_phase(const setup_obs_t *o)
{
    if (o->other)
        printf("  setup phase: unexpected kind %llu\n",
               (unsigned long long)o->other_kind);
    MOQ_TEST_CHECK_EQ_INT(o->ns_pub, 1);
    MOQ_TEST_CHECK_EQ_INT(o->sub_ok, 0);      /* nobody has subscribed yet */
    MOQ_TEST_CHECK_EQ_INT(o->other, 0);
    MOQ_TEST_CHECK(o->ann_valid);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)o->ns_count, 2u);
    MOQ_TEST_CHECK(o->ns_parts_match);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)o->ns_tokens, 0u);
    MOQ_TEST_CHECK(o->ns_tokens_null);
    MOQ_TEST_CHECK_EQ_INT(o->accept_rc, (int)MOQ_OK);
}

/* Phase 2's complete declared inventory. The response is for THIS subscription,
 * on the alias derived before ingress, and every body field is declared: the
 * catalog track has published generation 0 object 0, so a LARGEST_OBJECT
 * subscribe is answered with that largest; the sender sets no expiry, no track
 * properties and no dynamic groups. The announcement count is re-asserted, so a
 * duplicate arriving during this phase is caught here too. */
static void assert_subscribe_phase(const setup_obs_t *o, uint64_t sub_opaque,
                                   uint64_t want_alias)
{
    if (o->other)
        printf("  subscribe phase: unexpected kind %llu\n",
               (unsigned long long)o->other_kind);
    MOQ_TEST_CHECK_EQ_INT(o->sub_ok, 1);
    MOQ_TEST_CHECK_EQ_INT(o->ns_pub, 1);      /* still exactly one, no duplicate */
    MOQ_TEST_CHECK_EQ_INT(o->other, 0);
    MOQ_TEST_CHECK_EQ_U64(o->sok_sub, sub_opaque);
    MOQ_TEST_CHECK_EQ_U64(o->sok_alias, want_alias);
    MOQ_TEST_CHECK(o->sok_has_largest);
    MOQ_TEST_CHECK_EQ_U64(o->sok_lg, 0u);
    MOQ_TEST_CHECK_EQ_U64(o->sok_lo, 0u);
    MOQ_TEST_CHECK(!o->sok_has_expires);
    MOQ_TEST_CHECK_EQ_U64(o->sok_expires_ms, 0u);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)o->sok_props_len, 0u);
    MOQ_TEST_CHECK(!o->sok_dynamic_groups);
}

/* The setup window is fully claimed before any measured window opens: one more
 * classifying pass must find nothing left. */
static void assert_setup_window_closed(moq_session_t *srv, uint64_t now_us)
{
    setup_obs_t drained;
    memset(&drained, 0, sizeof(drained));
    drained.accept_rc = (int)MOQ_OK;
    classify_setup(srv, &drained, now_us);
    MOQ_TEST_CHECK_EQ_INT(drained.ns_pub, 0);
    MOQ_TEST_CHECK_EQ_INT(drained.sub_ok, 0);
    MOQ_TEST_CHECK_EQ_INT(drained.other, 0);
}

/* Establish a pair, a ready sender publishing one track with its initial
 * catalog stamped A, and a real catalog subscriber -- classifying phases 0, 1
 * and 2 completely and closing the setup window. Returns false (and leaves
 * nothing to clean up beyond f->sp) if the pair or sender could not be made. */
static bool fixture_setup(gen_fixture_t *f)
{
    memset(f, 0, sizeof(*f));
    moq_simpair_cfg_t sc = MOQ_SIMPAIR_CFG_INIT;
    sc.alloc = moq_alloc_default();
    sc.seed = 42;
    sc.initial_now_us = 1000;
    sc.client_send_request_capacity = true;
    sc.client_initial_request_capacity = 32;
    sc.server_send_request_capacity = true;
    sc.server_initial_request_capacity = 32;
    sc.version = MOQ_VERSION_DRAFT_18;
    MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_create(&sc, &f->sp), (int)MOQ_OK);
    MOQ_TEST_CHECK(f->sp != NULL);
    if (!f->sp) return false;
    moq_simpair_start(f->sp);
    moq_simpair_run_until_quiescent(f->sp, 8, NULL);
    f->cl = moq_simpair_client(f->sp);
    f->srv = moq_simpair_server(f->sp);
    f->t0 = moq_simpair_now_us(f->sp);

    /* PHASE 0 -- handshake. Both sides must show exactly the SETUP_COMPLETE
     * this fixture expects and nothing else; only then is it consumed. */
    expect_setup_only(f->cl, "client", MOQ_PERSPECTIVE_CLIENT,
                      MOQ_PERSPECTIVE_SERVER);
    expect_setup_only(f->srv, "server", MOQ_PERSPECTIVE_SERVER,
                      MOQ_PERSPECTIVE_CLIENT);

    g_ns_parts[0] = MOQ_BYTES_LITERAL("svc");
    g_ns_parts[1] = MOQ_BYTES_LITERAL("demo");
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns_parts, 2 };
    cfg.publish_tracks = false;          /* pull mode: the peer subscribes */
    cfg.catalog_refresh_interval_us = 1000000ull;
    f->s = moq_media_sender_test_new_cfg(&cfg);
    MOQ_TEST_CHECK(f->s != NULL);
    if (!f->s) return false;
    moq_media_sender_test_set_clock_ms(f->s, true, STAMP_A);
    add_trk(f->s, "v");

    /* PHASE 1 -- the announcement. */
    setup_obs_t obs;
    memset(&obs, 0, sizeof(obs));
    obs.accept_rc = (int)MOQ_ERR_INVAL;   /* until an accept actually runs */
    for (int c = 0; c < 12 && !moq_media_sender_is_ready(f->s); c++) {
        moq_media_sender_test_pump(f->s, f->cl, f->t0);
        moq_simpair_run_until_quiescent(f->sp, 8, NULL);
        classify_setup(f->srv, &obs, f->t0);
        moq_media_sender_test_pump(f->s, f->cl, f->t0);
        moq_simpair_run_until_quiescent(f->sp, 8, NULL);
    }
    MOQ_TEST_CHECK(moq_media_sender_is_ready(f->s));
    assert_namespace_phase(&obs);

    /* PHASE 2 -- a real subscriber on the catalog track. The alias the
     * SUBSCRIBE_OK must carry is DERIVED from the publisher session's state
     * before this subscribe is ingested, never adopted from the event. A zero
     * derivation would compare equal to an unset field, so it is checked. */
    f->want_alias = f->cl->profile->next_track_alias(f->cl);
    MOQ_TEST_CHECK(f->want_alias != 0);
    moq_subscribe_cfg_t subc;
    moq_subscribe_cfg_init(&subc);
    subc.track_namespace = (moq_namespace_t){ g_ns_parts, 2 };
    subc.track_name = MOQ_BYTES_LITERAL("catalog");
    subc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    subc.has_forward = true; subc.forward = true;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_subscribe(f->srv, &subc, f->t0, &f->sub), (int)MOQ_OK);
    MOQ_TEST_CHECK(f->sub._opaque != 0);
    for (int i = 0; i < 16 && obs.sub_ok == 0; i++) {
        moq_media_sender_test_pump(f->s, f->cl, f->t0);
        moq_simpair_run_until_quiescent(f->sp, 8, NULL);
        classify_setup(f->srv, &obs, f->t0);
    }
    assert_subscribe_phase(&obs, f->sub._opaque, f->want_alias);
    assert_setup_window_closed(f->srv, f->t0);
    return true;
}

/* Drain both queues after the sender is gone. Cleanup only -- whatever is left
 * here belongs to no measured window. */
static void fixture_teardown(gen_fixture_t *f)
{
    moq_event_t ev;
    if (f->cl) while (moq_session_poll_events(f->cl, &ev, 1) == 1) moq_event_cleanup(&ev);
    if (f->srv) while (moq_session_poll_events(f->srv, &ev, 1) == 1) moq_event_cleanup(&ev);
    if (f->sp) moq_simpair_destroy(f->sp);
}

static void test_refresh_is_a_real_instance(void)
{
    gen_fixture_t f;
    if (!fixture_setup(&f)) { fixture_teardown(&f); return; }
    moq_session_t *cl = f.cl, *srv = f.srv;
    moq_media_sender_t *s = f.s;
    uint64_t t0 = f.t0;
    moq_subscription_t sub = f.sub;
    moq_simpair_t *sp = f.sp;

    /* The baseline the refresh must reproduce, declared independently. */
    static const char *const one[] = { "v" };
    inv_t want_base;
    expect_baseline(&want_base, STAMP_A, one, 1);
    inv_t base;
    moq_rcbuf_t *base_buf = build_retained(s, &base);
    assert_inventory(&base, &want_base, "pre-refresh baseline");
    if (base_buf) moq_rcbuf_decref(base_buf);

    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 0);
    unsigned installs0 = moq_media_sender_test_retained_installs(s);

    /* PHASE 3 -- the refresh window. Nothing about the catalog changes; only
     * the refresh deadline arrives. The clock moves to B so the refreshed
     * INSTANCE is stamped B. Setup events are unexpected from here on. */
    moq_media_sender_test_set_clock_ms(s, true, STAMP_B);
    peer_stream_t ps;
    memset(&ps, 0, sizeof(ps));
    uint64_t td = t0 + 1000000ull;
    for (int i = 0; i < 8; i++) {
        moq_media_sender_test_pump(s, cl, td);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        classify_peer(srv, &ps);
    }

    /* Lifecycle counters, independent of the payload. */
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 1);
    MOQ_TEST_CHECK_EQ_U64(
        (uint64_t)moq_media_sender_test_retained_installs(s),
        (uint64_t)installs0 + 1u);

    /* Exactly one object, nothing else on the stream. */
    if (ps.unexpected)
        printf("  refresh window: unexpected kind %llu\n",
               (unsigned long long)ps.last_unexpected_kind);
    MOQ_TEST_CHECK_EQ_INT(ps.objects, 1);
    MOQ_TEST_CHECK(!ps.obj_overflow);
    MOQ_TEST_CHECK_EQ_INT(ps.unexpected, 0);

    if (ps.objects == 1) {
        /* Its complete public identity, declared from the fixture and from the
         * sender's own source contract: the catalog track is registered at
         * publisher priority 0 (media_sender.c "catalog leads delivery") and
         * every catalog object carries end_of_group (one subgroup per
         * generation), is a normal stream object, and has no properties. */
        obj_want_t w;
        memset(&w, 0, sizeof(w));
        w.sub_opaque = sub._opaque;
        w.pub_opaque = 0;               /* peer-initiated subscription: no pub */
        w.group_id = 1;                 /* the refresh cuts generation 1 */
        w.subgroup_id = 0;
        w.object_id = 0;
        w.publisher_priority = 0;
        w.status = (int)MOQ_OBJECT_NORMAL;
        w.end_of_group = true;
        w.datagram = false;
        w.have_payload = true;
        w.have_properties = false;
        assert_object_identity(&ps.obj[0], &w, "refresh object");

        /* Its content: the unchanged catalog, stamped B. */
        MOQ_TEST_CHECK(ps.obj[0].payload_len > 0);
        inv_t want_refresh;
        expect_baseline(&want_refresh, STAMP_B, one, 1);
        assert_inventory(&ps.obj[0].body, &want_refresh, "refresh payload");
        MOQ_TEST_CHECK_EQ_INT(
            inv_diff_except_stamp(&base, &ps.obj[0].body, "refresh vs base"), 0);
        MOQ_TEST_CHECK_EQ_U64(ps.obj[0].body.generated_at, STAMP_B);
        MOQ_TEST_CHECK(ps.obj[0].body.generated_at != base.generated_at);
    }

    moq_media_sender_test_free(s);
    fixture_teardown(&f);
}

/* -- 7: a generation that waits across clock movement keeps ITS stamp ---- *
 *
 * The published timestamp must come from the bytes that were staged, not from
 * the clock at commit time. A generation can be live-written at one instant and
 * committed at another -- the retained install is backpressured and retried on a
 * later pump -- so a stamp read at commit would label the committed catalog with
 * a time its own bytes do not carry. The next semantic comparison would then
 * rebuild at the published stamp, differ from the published bytes, and cut a
 * generation with nothing in it: the very defect this file exists to close,
 * reintroduced through the back door.
 *
 * The arm blocks a real refresh at the retained-install step (after its live
 * object is on the wire, before install and commit), moves the injected clock,
 * releases, and then asks BOTH shared-helper callers whether anything changed.
 */
static void test_blocked_generation_keeps_its_stamp(void)
{
    gen_fixture_t f;
    if (!fixture_setup(&f)) { fixture_teardown(&f); return; }
    moq_session_t *cl = f.cl, *srv = f.srv;
    moq_media_sender_t *s = f.s;
    uint64_t t0 = f.t0;
    moq_subscription_t sub = f.sub;
    moq_simpair_t *sp = f.sp;

    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 0);
    unsigned installs0 = moq_media_sender_test_retained_installs(s);

    /* The refresh is STAGED and live-written at B, then blocked before its
     * retained install -- the exact point where the generation starts waiting. */
    moq_media_sender_test_set_clock_ms(s, true, STAMP_B);
    moq_media_sender_test_block_retained_install_once(s);
    peer_stream_t blocked;
    memset(&blocked, 0, sizeof(blocked));
    uint64_t td = t0 + 1000000ull;
    /* EXACTLY ONE sender pump: it stages the refresh, live-writes the object,
     * and then hits the one-shot install block. A second pump here would simply
     * retry the install and commit, which is the next step, not this one. */
    moq_media_sender_test_pump(s, cl, td);
    for (int i = 0; i < 3; i++) {
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        classify_peer(srv, &blocked);
    }

    /* The object really went out, and it carries B. */
    MOQ_TEST_CHECK_EQ_INT(blocked.objects, 1);
    MOQ_TEST_CHECK(!blocked.obj_overflow);
    MOQ_TEST_CHECK_EQ_INT(blocked.unexpected, 0);
    if (blocked.objects == 1) {
        obj_want_t w;
        memset(&w, 0, sizeof(w));
        w.sub_opaque = sub._opaque;
        w.pub_opaque = 0;
        w.group_id = 1;
        w.subgroup_id = 0;
        w.object_id = 0;
        w.publisher_priority = 0;
        w.status = (int)MOQ_OBJECT_NORMAL;
        w.end_of_group = true;
        w.datagram = false;
        w.have_payload = true;
        w.have_properties = false;
        assert_object_identity(&blocked.obj[0], &w, "blocked refresh object");
        static const char *const one[] = { "v" };
        inv_t want_b;
        expect_baseline(&want_b, STAMP_B, one, 1);
        assert_inventory(&blocked.obj[0].body, &want_b, "blocked refresh payload");
        MOQ_TEST_CHECK_EQ_U64(blocked.obj[0].body.generated_at, STAMP_B);
    }

    /* ... and it is NOT committed: the group has not advanced, no install has
     * succeeded, and the staged generation is still pending. */
    size_t cursor = 0, count = 0;
    unsigned installs_blocked = 0;
    moq_media_sender_test_republish_live(s, &cursor, &count, &installs_blocked);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 0);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)installs_blocked, (uint64_t)installs0);
    /* EXACT geometry, not merely "something is pending". A refresh stages ONE
     * independent object, and the refusal is placed AFTER the live-write loop
     * and BEFORE the install -- so the cursor has advanced past that one object
     * while the generation is still uncommitted: cursor == count == 1. */
    MOQ_TEST_CHECK_EQ_U64((uint64_t)count, 1u);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)cursor, 1u);

    /* The clock moves while the generation waits. A commit that read the clock
     * here would stamp the published catalog C, though its bytes say B. */
    moq_media_sender_test_set_clock_ms(s, true, STAMP_C);
    peer_stream_t retry;
    memset(&retry, 0, sizeof(retry));
    for (int i = 0; i < 4; i++) {
        moq_media_sender_test_pump(s, cl, td);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        classify_peer(srv, &retry);
    }

    /* Exactly one commit, and no second copy of the object on the wire. */
    MOQ_TEST_CHECK_EQ_INT(retry.objects, 0);
    MOQ_TEST_CHECK_EQ_INT(retry.unexpected, 0);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 1);
    MOQ_TEST_CHECK_EQ_U64(
        (uint64_t)moq_media_sender_test_retained_installs(s),
        (uint64_t)installs0 + 1u);
    size_t count_after = 1;
    moq_media_sender_test_republish_live(s, NULL, &count_after, NULL);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)count_after, 0u);   /* committed */

    /* THE POINT. The clock now says C and the catalog is unchanged, so both
     * shared-helper callers must report a no-op. They can only do that if the
     * published stamp is the STAGED stamp B -- a comparison built at C against
     * bytes stamped B would differ, and the sender would cut an empty
     * generation. */
    MOQ_TEST_CHECK(!moq_media_sender_test_build_changed(s));
    moq_rcbuf_t *objs[3] = { NULL, NULL, NULL };
    size_t staged = moq_media_sender_test_stage(s, objs, 3);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)staged, 0u);
    for (size_t i = 0; i < staged; i++) if (objs[i]) moq_rcbuf_decref(objs[i]);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 1);
    MOQ_TEST_CHECK_EQ_U64(
        (uint64_t)moq_media_sender_test_retained_installs(s),
        (uint64_t)installs0 + 1u);

    moq_media_sender_test_free(s);
    fixture_teardown(&f);
    MOQ_TEST_PASS("generatedat.blocked_generation_keeps_its_stamp");
}

/* Each case reports its OWN verdict. MOQ_TEST_PASS goes quiet once anything
 * has failed, which would hide whether the later contracts still hold while a
 * failure stands -- and "the refresh and completion paths are unaffected" is a
 * claim this file has to make visibly, not by silence. */
#define RUN_CASE(label, call)                                                 \
    do {                                                                      \
        int before_ = failures;                                               \
        call;                                                                 \
        printf("CASE %-34s %s\n", (label),                                    \
               failures == before_ ? "pass" : "FAIL");                        \
    } while (0)

int main(void)
{
    RUN_CASE("noop_dedup_same_ms (control)", test_noop_dedup(true));
    RUN_CASE("noop_dedup_across_ms",         test_noop_dedup(false));
    RUN_CASE("real_add_still_changes",       test_real_add_still_changes());
    RUN_CASE("completion_empty_to_empty",    test_completion_still_generates(true));
    RUN_CASE("completion_populated_to_empty", test_completion_still_generates(false));
    RUN_CASE("refresh_is_a_real_instance",   test_refresh_is_a_real_instance());
    RUN_CASE("clock_seam_instance_owned",    test_clock_seam_is_instance_owned());
    RUN_CASE("blocked_generation_keeps_stamp",
             test_blocked_generation_keeps_its_stamp());
    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
