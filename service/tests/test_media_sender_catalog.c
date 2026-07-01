/*
 * White-box tests for the sender's catalog-content model (S1): the catalog the
 * sender would publish after post-ready add/remove mutations. Deterministic,
 * no network -- it pins coalescing (multiple mutations reflected in one build)
 * and the no-op dedup (an add+remove that nets to no change builds identical
 * bytes). The pump-driven wire republish (group numbering, retained group,
 * staged backpressure) is covered by the loopback tests in test_media_sender.c.
 */
#include <moq/media_sender.h>
#include <moq/endpoint.h>
#include <moq/msf.h>
#include <moq/rcbuf.h>
#include "test_support.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <string.h>
#include <pthread.h>
#include <time.h>

static int failures = 0;

/* Test seam (media_sender.c, MOQ_MEDIA_SENDER_TESTING). */
moq_media_sender_t *moq_media_sender_test_new(void);
void moq_media_sender_test_free(moq_media_sender_t *s);
void moq_media_sender_test_set_ready(moq_media_sender_t *s);
void moq_media_sender_test_complete(moq_media_sender_t *s);
moq_result_t moq_media_sender_test_build_catalog(moq_media_sender_t *s,
                                                 moq_rcbuf_t **out);
bool moq_media_sender_test_catalog_dirty(const moq_media_sender_t *s);
void moq_media_sender_test_mark_published(moq_media_sender_t *s);
bool moq_media_sender_test_build_changed(moq_media_sender_t *s);
bool moq_media_sender_test_track_finish_pending(const moq_media_sender_t *s,
                                                const moq_media_track_t *track);
bool moq_media_sender_test_conversion_finish_pending(const moq_media_sender_t *s);
bool moq_media_sender_test_run_finish_conversions(moq_media_sender_t *s);
size_t moq_media_sender_test_stage(moq_media_sender_t *s,
                                   moq_rcbuf_t **objs, size_t cap);
void moq_media_sender_test_mark_registered(moq_media_sender_t *s);
moq_result_t moq_media_sender_test_validate_cfg(const moq_media_sender_cfg_t *cfg);
/* Lock-order regression seams (media_sender.c + endpoint.c, _TESTING). */
void moq_media_sender_test_set_ep(moq_media_sender_t *s, moq_endpoint_t *ep);
bool moq_media_sender_test_endpoint_closed(moq_media_sender_t *s);
moq_endpoint_t *moq_endpoint_test_make_bare(void);
void moq_endpoint_test_free_bare(moq_endpoint_t *ep);
void moq_endpoint_test_lock_mu(moq_endpoint_t *ep);
void moq_endpoint_test_unlock_mu(moq_endpoint_t *ep);

/* Worker: run the sender's BLOCK_TIMEOUT closed-check; flag completion. The
 * check must be lock-free and return immediately; taking ep->mu (held by the
 * main thread) would block here (lock inversion). */
struct lockorder_arg { moq_media_sender_t *s; atomic_bool done; };
static void *lockorder_worker(void *p)
{
    struct lockorder_arg *a = (struct lockorder_arg *)p;
    (void)moq_media_sender_test_endpoint_closed(a->s);
    atomic_store(&a->done, true);
    return NULL;
}

static moq_media_track_t *add_trk(moq_media_sender_t *s, const char *name,
                                  bool sap)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };  /* MSF-01 5.2.18 */
    tc.bitrate = 1500000;                                    /* MSF-01 5.2.22 */
    tc.is_live = true;
    tc.emit_sap_timeline = sap;
    moq_media_track_t *t = NULL;
    moq_result_t rc = moq_media_sender_add_track(s, &tc, &t);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    return t;
}

/* Add a CMAF track carrying an explicit CMSF §3.2 altGroup (switching-set
 * member). No init segment: a CMAF track without init data is still valid here,
 * and altGroup authoring is independent of it. */
static moq_media_track_t *add_cmaf_altgroup(moq_media_sender_t *s,
                                            const char *name, int alt_group)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_CMAF;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"avc1.640028", 11 };
    tc.bitrate = 1500000;
    tc.timescale = 90000;
    tc.is_live = true;
    tc.has_alt_group = true;
    tc.alt_group = alt_group;
    moq_media_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                          (int)MOQ_OK);
    return t;
}

/* Build + parse; report whether track `name` is present, and (via out params)
 * whether its catalog entry carried altGroup and its value. */
static bool built_track_alt_group(moq_media_sender_t *s, const char *name,
                                  bool *has_alt, int *alt)
{
    *has_alt = false; *alt = 0;
    moq_rcbuf_t *j = NULL;
    if (moq_media_sender_test_build_catalog(s, &j) != MOQ_OK || !j) return false;
    const moq_alloc_t *al = moq_alloc_default();
    moq_msf_catalog_t c;
    bool found = false;
    if (moq_msf_catalog_parse(al,
            (moq_bytes_t){ moq_rcbuf_data(j), moq_rcbuf_len(j) }, &c) == MOQ_OK) {
        size_t nl = strlen(name);
        for (size_t i = 0; i < c.track_count; i++)
            if (c.tracks[i].name.len == nl &&
                memcmp(c.tracks[i].name.data, name, nl) == 0) {
                found = true;
                *has_alt = c.tracks[i].has_alt_group;
                *alt = c.tracks[i].alt_group;
                break;
            }
        moq_msf_catalog_cleanup(al, &c);
    }
    moq_rcbuf_decref(j);
    return found;
}

/* True if the currently-built catalog contains a track named `name`. */
static bool built_has(moq_media_sender_t *s, const char *name)
{
    moq_rcbuf_t *j = NULL;
    if (moq_media_sender_test_build_catalog(s, &j) != MOQ_OK || !j) return false;
    const moq_alloc_t *al = moq_alloc_default();
    moq_msf_catalog_t c;
    bool found = false;
    if (moq_msf_catalog_parse(al,
            (moq_bytes_t){ moq_rcbuf_data(j), moq_rcbuf_len(j) }, &c) == MOQ_OK) {
        size_t nl = strlen(name);
        for (size_t i = 0; i < c.track_count; i++)
            if (c.tracks[i].name.len == nl &&
                memcmp(c.tracks[i].name.data, name, nl) == 0) { found = true; break; }
        moq_msf_catalog_cleanup(al, &c);
    }
    moq_rcbuf_decref(j);
    return found;
}

static size_t built_track_count(moq_media_sender_t *s)
{
    moq_rcbuf_t *j = NULL;
    if (moq_media_sender_test_build_catalog(s, &j) != MOQ_OK || !j) return (size_t)-1;
    const moq_alloc_t *al = moq_alloc_default();
    moq_msf_catalog_t c;
    size_t n = (size_t)-1;
    if (moq_msf_catalog_parse(al,
            (moq_bytes_t){ moq_rcbuf_data(j), moq_rcbuf_len(j) }, &c) == MOQ_OK) {
        n = c.track_count;
        moq_msf_catalog_cleanup(al, &c);
    }
    moq_rcbuf_decref(j);
    return n;
}

/* Build + parse; report is_complete via *complete and return track_count
 * (SIZE_MAX on failure). */
static size_t built_complete_count(moq_media_sender_t *s, bool *complete)
{
    *complete = false;
    moq_rcbuf_t *j = NULL;
    if (moq_media_sender_test_build_catalog(s, &j) != MOQ_OK || !j) return (size_t)-1;
    const moq_alloc_t *al = moq_alloc_default();
    moq_msf_catalog_t c;
    size_t n = (size_t)-1;
    if (moq_msf_catalog_parse(al,
            (moq_bytes_t){ moq_rcbuf_data(j), moq_rcbuf_len(j) }, &c) == MOQ_OK) {
        n = c.track_count;
        *complete = c.is_complete;
        moq_msf_catalog_cleanup(al, &c);
    }
    moq_rcbuf_decref(j);
    return n;
}

/* Add a non-live track carrying trackDuration; returns the handle (or asserts
 * the expected add_track result when want_rc != MOQ_OK). */
static moq_media_track_t *add_vod_trk(moq_media_sender_t *s, const char *name,
                                      bool is_live, uint64_t dur_ms,
                                      moq_result_t want_rc)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };  /* MSF-01 5.2.18 */
    tc.bitrate = 1500000;                                    /* MSF-01 5.2.22 */
    tc.is_live = is_live;
    tc.has_track_duration = true;
    tc.track_duration_ms = dur_ms;
    moq_media_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                          (int)want_rc);
    return t;
}

/* Add a media track with a generated media-timeline sibling. */
static moq_media_track_t *add_mt_trk(moq_media_sender_t *s, const char *name)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };  /* MSF-01 5.2.18 */
    tc.bitrate = 1500000;                                    /* MSF-01 5.2.22 */
    tc.is_live = true;
    tc.emit_media_timeline = true;
    moq_media_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                          (int)MOQ_OK);
    return t;
}

/* Add a plain RAW app track with an explicit name; assert the add result. */
static void add_named(moq_media_sender_t *s, const char *name,
                      moq_result_t want_rc)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };  /* MSF-01 5.2.18 */
    tc.bitrate = 1500000;                                    /* MSF-01 5.2.22 */
    tc.is_live = true;
    moq_media_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                          (int)want_rc);
}

/* Build + parse; assert the named generated track is a §7.2 mediatimeline:
 * packaging "mediatimeline", mimeType "application/json", depends [dep], no
 * eventType. */
static void check_mt_catalog(moq_media_sender_t *s, const char *name,
                             const char *dep)
{
    moq_rcbuf_t *j = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &j),
                          (int)MOQ_OK);
    if (!j) return;
    const moq_alloc_t *al = moq_alloc_default();
    moq_msf_catalog_t c;
    MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_parse(al,
        (moq_bytes_t){ moq_rcbuf_data(j), moq_rcbuf_len(j) }, &c), (int)MOQ_OK);
    size_t nl = strlen(name);
    const moq_msf_track_t *tr = NULL;
    for (size_t i = 0; i < c.track_count; i++)
        if (c.tracks[i].name.len == nl &&
            memcmp(c.tracks[i].name.data, name, nl) == 0) { tr = &c.tracks[i]; break; }
    MOQ_TEST_CHECK(tr != NULL);
    if (tr) {
        MOQ_TEST_CHECK(tr->packaging.len == 13 &&
            memcmp(tr->packaging.data, "mediatimeline", 13) == 0);
        MOQ_TEST_CHECK(tr->has_mime_type && tr->mime_type.len == 16 &&
            memcmp(tr->mime_type.data, "application/json", 16) == 0);
        MOQ_TEST_CHECK(!tr->has_event_type);
        size_t dl = strlen(dep);
        MOQ_TEST_CHECK(tr->depends_count == 1 && tr->depends[0].len == dl &&
            memcmp(tr->depends[0].data, dep, dl) == 0);
    }
    moq_msf_catalog_cleanup(al, &c);
    moq_rcbuf_decref(j);
}

int main(void)
{
    /* -- 1. coalescing: two post-ready adds both appear in one build ---- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        MOQ_TEST_CHECK(s != NULL);
        moq_media_track_t *v = add_trk(s, "v", false);
        MOQ_TEST_CHECK(v != NULL);
        moq_media_sender_test_set_ready(s);

        /* Two adds before any republish: the next build reflects both. */
        MOQ_TEST_CHECK(add_trk(s, "a", false) != NULL);
        MOQ_TEST_CHECK(add_trk(s, "b", false) != NULL);
        MOQ_TEST_CHECK(moq_media_sender_test_catalog_dirty(s));
        MOQ_TEST_CHECK(built_has(s, "v"));
        MOQ_TEST_CHECK(built_has(s, "a"));
        MOQ_TEST_CHECK(built_has(s, "b"));
        MOQ_TEST_CHECK_EQ_U64(built_track_count(s), 3);
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.coalesce_adds");
    }

    /* -- 2. remove excludes the track from the build ------------------- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        moq_media_track_t *v = add_trk(s, "v", false);
        add_trk(s, "a", false);
        moq_media_sender_test_set_ready(s);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, v),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(!built_has(s, "v"));
        MOQ_TEST_CHECK(built_has(s, "a"));
        /* A write to a removed track is refused (WRONG_STATE). */
        {
            moq_rcbuf_t *pl = NULL;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_rcbuf_create(moq_alloc_default(),
                    (const uint8_t *)"x", 1, &pl), (int)MOQ_OK);
            moq_media_send_object_t o;
            memset(&o, 0, sizeof(o));
            o.struct_size = sizeof(o);
            o.payload = pl;
            o.is_sync = true; o.starts_group = true;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, v, &o),
                                  (int)MOQ_ERR_WRONG_STATE);
            moq_rcbuf_decref(pl);   /* refused: no ownership taken */
        }
        /* A second remove of the same handle is a terminal-state error. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, v),
                              (int)MOQ_ERR_WRONG_STATE);
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.remove_excludes");
    }

    /* -- 3. add then remove the same track nets to no change (dedup) --- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        add_trk(s, "v", false);
        add_trk(s, "a", false);
        moq_media_sender_test_set_ready(s);

        /* Baseline bytes. */
        moq_rcbuf_t *b1 = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &b1),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(b1 != NULL);

        /* Add x, then remove x: the resolved catalog is unchanged. */
        moq_media_track_t *x = add_trk(s, "x", false);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, x),
                              (int)MOQ_OK);
        moq_rcbuf_t *b2 = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &b2),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(b2 != NULL);
        if (b1 && b2) {
            MOQ_TEST_CHECK_EQ_U64(moq_rcbuf_len(b1), moq_rcbuf_len(b2));
            MOQ_TEST_CHECK(moq_rcbuf_len(b1) == moq_rcbuf_len(b2) &&
                memcmp(moq_rcbuf_data(b1), moq_rcbuf_data(b2),
                       moq_rcbuf_len(b1)) == 0);
        }
        if (b1) moq_rcbuf_decref(b1);
        if (b2) moq_rcbuf_decref(b2);

        /* The dedup decision itself: with {v,a} as the published baseline, an
         * add+remove that nets to {v,a} would NOT cut a new generation, while a
         * real add would. */
        moq_media_sender_test_mark_published(s);   /* baseline = {v,a} */
        moq_media_track_t *x2 = add_trk(s, "x2", false);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, x2),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(!moq_media_sender_test_build_changed(s));  /* no-op */
        add_trk(s, "y", false);                    /* a real change */
        MOQ_TEST_CHECK(moq_media_sender_test_build_changed(s));

        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.add_remove_noop_dedup");
    }

    /* -- 3b. pre-ready remove keeps the track out of the initial catalog - */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();   /* not ready */
        moq_media_track_t *v = add_trk(s, "v", false);
        add_trk(s, "keep", false);
        /* Removed before ready: excluded from the initial catalog (and from the
         * ready-time pub-track registration -- see sender_hook). */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, v),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(!built_has(s, "v"));
        MOQ_TEST_CHECK(built_has(s, "keep"));
        MOQ_TEST_CHECK_EQ_U64(built_track_count(s), 1);
        /* The removed handle is inert: writes are refused. */
        {
            moq_rcbuf_t *pl = NULL;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_rcbuf_create(moq_alloc_default(),
                    (const uint8_t *)"x", 1, &pl), (int)MOQ_OK);
            moq_media_send_object_t o;
            memset(&o, 0, sizeof(o));
            o.struct_size = sizeof(o);
            o.payload = pl; o.is_sync = true; o.starts_group = true;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, v, &o),
                                  (int)MOQ_ERR_WRONG_STATE);
            moq_rcbuf_decref(pl);
        }
        /* Same for an emit_sap_timeline track: neither it nor its .sap sibling
         * enters the initial catalog. */
        moq_media_track_t *w = add_trk(s, "w", true);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, w),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(!built_has(s, "w"));
        MOQ_TEST_CHECK(!built_has(s, "w.sap"));
        MOQ_TEST_CHECK_EQ_U64(built_track_count(s), 1);   /* only "keep" */
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.pre_ready_remove_excluded");
    }

    /* -- 4. emit_sap_timeline add brings in media + "<name>.sap" ------- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        moq_media_sender_test_set_ready(s);
        moq_media_track_t *v = add_trk(s, "v", true);   /* sap timeline */
        MOQ_TEST_CHECK(v != NULL);
        MOQ_TEST_CHECK(built_has(s, "v"));
        MOQ_TEST_CHECK(built_has(s, "v.sap"));
        MOQ_TEST_CHECK_EQ_U64(built_track_count(s), 2);

        /* Removing the media track removes its generated timeline too. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, v),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(!built_has(s, "v"));
        MOQ_TEST_CHECK(!built_has(s, "v.sap"));
        MOQ_TEST_CHECK_EQ_U64(built_track_count(s), 0);
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.sap_timeline_pair");
    }

    /* -- 5. a generated eventtimeline track is not user-removable ------ */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        moq_media_sender_test_set_ready(s);
        moq_media_track_t *v = add_trk(s, "v", true);
        (void)v;
        /* The "v.sap" handle is internal -- the app never gets it. We can only
         * assert via the public surface that removing v removes the pair
         * (covered above). Here, confirm a fresh foreign handle is rejected. */
        moq_media_track_t *foreign = (moq_media_track_t *)(uintptr_t)0x1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, foreign),
                              (int)MOQ_ERR_INVAL);
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.foreign_remove_rejected");
    }

    /* -- emit_media_timeline add brings in media + "<name>.timeline" --- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        moq_media_sender_test_set_ready(s);
        moq_media_track_t *v = add_mt_trk(s, "v");
        MOQ_TEST_CHECK(v != NULL);
        MOQ_TEST_CHECK(built_has(s, "v"));
        MOQ_TEST_CHECK(built_has(s, "v.timeline"));
        MOQ_TEST_CHECK_EQ_U64(built_track_count(s), 2);
        /* The sibling's §7.2 catalog shape: mediatimeline + json + depends, no
         * eventType. */
        check_mt_catalog(s, "v.timeline", "v");

        /* Removing the media track removes its generated timeline too. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, v),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(!built_has(s, "v"));
        MOQ_TEST_CHECK(!built_has(s, "v.timeline"));
        MOQ_TEST_CHECK_EQ_U64(built_track_count(s), 0);
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.media_timeline_pair");
    }

    /* -- both timelines on one track: ".sap" AND ".timeline" siblings -- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        moq_media_sender_test_set_ready(s);
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };  /* MSF-01 */
        tc.bitrate = 1500000;
        tc.is_live = true;
        tc.emit_sap_timeline = true;
        tc.emit_media_timeline = true;
        moq_media_track_t *v = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &v),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(built_has(s, "v"));
        MOQ_TEST_CHECK(built_has(s, "v.sap"));
        MOQ_TEST_CHECK(built_has(s, "v.timeline"));
        MOQ_TEST_CHECK_EQ_U64(built_track_count(s), 3);
        /* Removing the media track removes BOTH siblings. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, v),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(built_track_count(s), 0);
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.both_timelines_pair");
    }

    /* -- ".timeline" name collisions, both directions (mirror SAP) ----- */
    {
        /* Generated "v.timeline" then an explicit app "v.timeline" collides. */
        moq_media_sender_t *s = moq_media_sender_test_new();
        moq_media_sender_test_set_ready(s);
        MOQ_TEST_CHECK(add_mt_trk(s, "v") != NULL);
        add_named(s, "v.timeline", MOQ_ERR_INVAL);
        moq_media_sender_test_free(s);

        /* Explicit app "w.timeline" then "w" with emit_media_timeline collides
         * (the generated name is already taken). */
        s = moq_media_sender_test_new();
        moq_media_sender_test_set_ready(s);
        add_named(s, "w.timeline", MOQ_OK);
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)"w", 1 };
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };  /* MSF-01 */
        tc.bitrate = 1500000;
        tc.is_live = true;
        tc.emit_media_timeline = true;
        moq_media_track_t *w = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &w),
                              (int)MOQ_ERR_INVAL);
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.media_timeline_collision");
    }

    /* -- pre-ready remove excludes the media track and its .timeline --- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        moq_media_track_t *v = add_mt_trk(s, "v");     /* pre-ready */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, v),
                              (int)MOQ_OK);
        moq_media_sender_test_set_ready(s);
        MOQ_TEST_CHECK(!built_has(s, "v"));
        MOQ_TEST_CHECK(!built_has(s, "v.timeline"));
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.media_timeline_pre_ready_remove");
    }

    /* -- 6. non-live track carries trackDuration in the build ---------- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        moq_media_track_t *v = add_vod_trk(s, "v", false, 8072340, MOQ_OK);
        MOQ_TEST_CHECK(v != NULL);
        moq_rcbuf_t *j = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &j),
                              (int)MOQ_OK);
        const moq_alloc_t *al = moq_alloc_default();
        moq_msf_catalog_t c;
        MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_parse(al,
            (moq_bytes_t){ moq_rcbuf_data(j), moq_rcbuf_len(j) }, &c),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(c.track_count, 1);
        MOQ_TEST_CHECK(!c.tracks[0].is_live);
        MOQ_TEST_CHECK(c.tracks[0].has_track_duration &&
                       c.tracks[0].track_duration_ms == 8072340);
        moq_msf_catalog_cleanup(al, &c);
        moq_rcbuf_decref(j);
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.vod_track_duration");
    }

    /* -- 7. trackDuration on a live track is rejected (§5.2.35) -------- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        MOQ_TEST_CHECK(add_vod_trk(s, "v", true, 1000, MOQ_ERR_INVAL) == NULL);
        MOQ_TEST_CHECK_EQ_U64(built_track_count(s), 0);  /* nothing added */
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.live_track_duration_rejected");
    }

    /* -- 8. completing build emits isComplete:true + empty tracks ------ */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        add_trk(s, "v", true);     /* media + generated "v.sap" */
        add_trk(s, "a", false);
        moq_media_sender_test_set_ready(s);
        moq_media_sender_test_mark_published(s);   /* baseline carries tracks */
        moq_media_sender_test_complete(s);
        MOQ_TEST_CHECK(moq_media_sender_test_catalog_dirty(s));
        MOQ_TEST_CHECK(moq_media_sender_test_build_changed(s));  /* != baseline */
        bool complete = false;
        size_t n = built_complete_count(s, &complete);
        MOQ_TEST_CHECK(complete);
        MOQ_TEST_CHECK_EQ_U64(n, 0);               /* empty tracks */
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.completing_terminal_build");
    }

    /* -- 9. completion supersedes a pending add; no track resurrected, and
     *    a post-complete add is refused -------------------------------- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        add_trk(s, "v", false);
        moq_media_sender_test_set_ready(s);
        add_trk(s, "late", false);            /* pending add, not published */
        moq_media_sender_test_complete(s);
        bool complete = false;
        size_t n = built_complete_count(s, &complete);
        MOQ_TEST_CHECK(complete);
        MOQ_TEST_CHECK_EQ_U64(n, 0);          /* "late" not resurrected */
        /* post-complete add is refused (terminal). */
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)"x", 1 };
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };  /* MSF-01 */
        tc.bitrate = 1500000;
        tc.is_live = true;
        moq_media_track_t *x = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &x),
                              (int)MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK(x == NULL);
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.complete_supersedes_pending");
    }

    /* -- 10. prefix-safe cfg: a pre-V3 caller (struct_size stops before
     *    has_track_duration) still adds, and the appended duration field
     *    defaults absent even though the (ignored) trailing bytes set it ---- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)"old", 3 };
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };  /* MSF-01 */
        tc.bitrate = 1500000;
        tc.is_live = true;
        /* Fields beyond the simulated old size carry values that WOULD be
         * rejected (trackDuration on a live track) if they were read. */
        tc.has_track_duration = true;
        tc.track_duration_ms = 999;
        tc.struct_size =
            (uint32_t)offsetof(moq_media_track_cfg_t, has_track_duration);
        moq_media_track_t *t = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                              (int)MOQ_OK);   /* accepted: duration not read */
        MOQ_TEST_CHECK(t != NULL);
        moq_rcbuf_t *j = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &j),
                              (int)MOQ_OK);
        const moq_alloc_t *al = moq_alloc_default();
        moq_msf_catalog_t c;
        MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_parse(al,
            (moq_bytes_t){ moq_rcbuf_data(j), moq_rcbuf_len(j) }, &c),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(c.track_count, 1);
        MOQ_TEST_CHECK(!c.tracks[0].has_track_duration);   /* defaulted absent */
        moq_msf_catalog_cleanup(al, &c);
        moq_rcbuf_decref(j);
        /* A struct_size below the required prefix is rejected. */
        tc.struct_size = 4;
        moq_media_track_t *t2 = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t2),
                              (int)MOQ_ERR_INVAL);
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.cfg_prefix_safe");
    }

    /* -- prefix-safe cfg: a V3 caller (struct_size stops before the appended
     *    media-timeline fields) is not misread. Trailing bytes set
     *    emit_media_timeline=true, but add_track must ignore them => no
     *    "<name>.timeline" sibling is generated. Guards the append-only ABI. */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        moq_media_sender_test_set_ready(s);
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)"v3", 2 };
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };  /* MSF-01 */
        tc.bitrate = 1500000;
        tc.is_live = true;
        /* A V3 header ends at track_duration_ms; simulate that prefix. */
        tc.struct_size =
            (uint32_t)offsetof(moq_media_track_cfg_t, emit_media_timeline);
        /* The bytes the V3 caller never had, set so a misread would generate. */
        tc.emit_media_timeline = true;
        tc.media_timeline_history_groups = 4;
        moq_media_track_t *t = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(t != NULL);
        MOQ_TEST_CHECK(built_has(s, "v3"));
        MOQ_TEST_CHECK(!built_has(s, "v3.timeline"));   /* not misread */
        MOQ_TEST_CHECK_EQ_U64(built_track_count(s), 1);
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.cfg_prefix_safe_media_timeline");
    }

    /* -- 11. convert_to_vod arms the §11.3 step-1 finish gate: the conversion
     *    catalog republish is gated until subscribers are finished, and
     *    catalog_dirty is preserved across the gate (never lost, never early). */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        moq_media_track_t *v = add_trk(s, "v", false);
        moq_media_track_t *a = add_trk(s, "a", false);
        moq_media_sender_test_set_ready(s);
        moq_media_sender_test_mark_published(s);   /* baseline = live catalog */

        moq_media_vod_track_t items[2] = { { v, 5000 }, { a, 7000 } };
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_convert_to_vod(s, items, 2),
                              (int)MOQ_OK);

        /* The conversion changed the catalog AND armed the step-1 finish on both
         * tracks, so the pump gates the republish until the finish completes. */
        MOQ_TEST_CHECK(moq_media_sender_test_catalog_dirty(s));
        MOQ_TEST_CHECK(moq_media_sender_test_build_changed(s));   /* VOD != base */
        MOQ_TEST_CHECK(moq_media_sender_test_track_finish_pending(s, v));
        MOQ_TEST_CHECK(moq_media_sender_test_track_finish_pending(s, a));
        MOQ_TEST_CHECK(moq_media_sender_test_conversion_finish_pending(s));

        /* Run the pump's finish step: with no live subscribers (white-box) it
         * completes immediately, clearing the gate WITHOUT dropping
         * catalog_dirty -- so the conversion catalog publishes next, not early
         * and not lost. */
        MOQ_TEST_CHECK(moq_media_sender_test_run_finish_conversions(s));
        MOQ_TEST_CHECK(!moq_media_sender_test_conversion_finish_pending(s));
        MOQ_TEST_CHECK(!moq_media_sender_test_track_finish_pending(s, v));
        MOQ_TEST_CHECK(!moq_media_sender_test_track_finish_pending(s, a));
        MOQ_TEST_CHECK(moq_media_sender_test_catalog_dirty(s));   /* preserved */

        /* The (now ungated) build carries both tracks as VOD, same tuples. */
        moq_rcbuf_t *j = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &j),
                              (int)MOQ_OK);
        const moq_alloc_t *al = moq_alloc_default();
        moq_msf_catalog_t c;
        MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_parse(al,
            (moq_bytes_t){ moq_rcbuf_data(j), moq_rcbuf_len(j) }, &c),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(c.track_count, 2);
        for (size_t i = 0; i < c.track_count; i++) {
            MOQ_TEST_CHECK(!c.tracks[i].is_live);
            MOQ_TEST_CHECK(c.tracks[i].has_track_duration);
        }
        moq_msf_catalog_cleanup(al, &c);
        moq_rcbuf_decref(j);

        /* Repeat conversion on an already-converted track is still rejected. */
        moq_media_vod_track_t again = { v, 9999 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_convert_to_vod(s, &again, 1),
                              (int)MOQ_ERR_INVAL);

        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.convert_finish_gate");
    }

    /* -- 12. A live->VOD conversion COALESCED with an add must still carry the
     *    VOD metadata on the wire. Staging diffs only add/remove membership, so
     *    a metadata-only change on a surviving tuple cannot ride an add/remove
     *    delta; the generation must fall back to a full independent catalog.
     *    Reconstruct the staged wire generation exactly as a receiver would
     *    (object 0 = independent base, objects 1..N = deltas applied in order)
     *    and assert the converted track reads back as VOD. */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        moq_media_track_t *v = add_trk(s, "v", false);
        (void)add_trk(s, "a", false);
        moq_media_sender_test_set_ready(s);
        moq_media_sender_test_mark_registered(s);  /* {v,a} registered */
        moq_media_sender_test_mark_published(s);   /* baseline = {v,a} live */

        /* Coalesce a live->VOD conversion of v with a post-ready add of "late". */
        moq_media_vod_track_t item = { v, 5000 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_convert_to_vod(s, &item, 1),
                              (int)MOQ_OK);
        (void)add_trk(s, "late", false);
        moq_media_sender_test_mark_registered(s);  /* "late" registered too */

        /* Stage the generation and reconstruct it as a receiver would. */
        moq_rcbuf_t *objs[3] = { NULL, NULL, NULL };
        size_t nobj = moq_media_sender_test_stage(s, objs, 3);
        MOQ_TEST_CHECK(nobj >= 1);

        const moq_alloc_t *al = moq_alloc_default();
        moq_msf_catalog_t eff;
        MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_parse(al,
            (moq_bytes_t){ moq_rcbuf_data(objs[0]), moq_rcbuf_len(objs[0]) },
            &eff), (int)MOQ_OK);
        for (size_t i = 1; i < nobj; i++) {
            moq_msf_catalog_t delta, merged;
            MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_parse(al,
                (moq_bytes_t){ moq_rcbuf_data(objs[i]), moq_rcbuf_len(objs[i]) },
                &delta), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_apply_delta(al, &eff,
                &delta, &merged), (int)MOQ_OK);
            moq_msf_catalog_cleanup(al, &delta);
            moq_msf_catalog_cleanup(al, &eff);
            eff = merged;
        }

        /* The receiver-effective catalog must show v converted to VOD and the
         * coalesced add present. Pre-fix the delta path omitted the conversion,
         * leaving v live with no trackDuration. */
        const moq_msf_track_t *vt = NULL, *lt = NULL;
        for (size_t i = 0; i < eff.track_count; i++) {
            if (eff.tracks[i].name.len == 1 &&
                eff.tracks[i].name.data[0] == 'v') vt = &eff.tracks[i];
            if (eff.tracks[i].name.len == 4 &&
                memcmp(eff.tracks[i].name.data, "late", 4) == 0) lt = &eff.tracks[i];
        }
        MOQ_TEST_CHECK(vt != NULL);
        MOQ_TEST_CHECK(vt && !vt->is_live);
        MOQ_TEST_CHECK(vt && vt->has_track_duration);
        MOQ_TEST_CHECK(vt && vt->track_duration_ms == 5000);
        MOQ_TEST_CHECK(lt != NULL);              /* coalesced add still present */

        moq_msf_catalog_cleanup(al, &eff);
        for (size_t i = 0; i < nobj; i++)
            if (objs[i]) moq_rcbuf_decref(objs[i]);
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.vod_conversion_coalesced_add");
    }

    /* -- MSF-01 required audio/video catalog fields -------------------- *
     * add_track must reject a track missing a field its codec requires, so the
     * sender never authors an invalid-by-construction catalog: codec (5.2.18)
     * and maximum bitrate (5.2.22) for both media types, plus samplerate
     * (5.2.28) and channelConfig (5.2.29) for audio. */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        moq_media_track_cfg_t tc;
        moq_media_track_t *t;

        /* Valid video: codec + bitrate => accepted. */
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)"vid", 3 };
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };
        tc.bitrate = 1500000;
        t = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(t != NULL);

        /* Video missing codec => rejected (5.2.18). */
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)"vnc", 3 };
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.bitrate = 1500000;                  /* codec omitted */
        t = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                              (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(t == NULL);

        /* Video missing bitrate => rejected (5.2.22). */
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)"vnb", 3 };
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };  /* bitrate 0 */
        t = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                              (int)MOQ_ERR_INVAL);

        /* Valid audio: codec + bitrate + samplerate + channelConfig => accepted. */
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)"aud", 3 };
        tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = (moq_bytes_t){ (const uint8_t *)"opus", 4 };
        tc.bitrate = 32000;
        tc.samplerate = 48000;
        tc.channel_config = (moq_bytes_t){ (const uint8_t *)"2", 1 };
        t = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                              (int)MOQ_OK);

        /* Audio missing samplerate => rejected (5.2.28). */
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)"ans", 3 };
        tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = (moq_bytes_t){ (const uint8_t *)"opus", 4 };
        tc.bitrate = 32000;
        tc.channel_config = (moq_bytes_t){ (const uint8_t *)"2", 1 };  /* no sr */
        t = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                              (int)MOQ_ERR_INVAL);

        /* Audio missing channelConfig => rejected (5.2.29). */
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)"anc", 3 };
        tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = (moq_bytes_t){ (const uint8_t *)"opus", 4 };
        tc.bitrate = 32000;
        tc.samplerate = 48000;                 /* channelConfig omitted */
        t = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                              (int)MOQ_ERR_INVAL);

        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.msf_required_av_fields");
    }

    /* -- lock-order: BLOCK_TIMEOUT closed-check must not take ep->mu ------- *
     * Regression for the AB/BA inversion (network thread: ep->mu -> hook ->
     * s->mu; writer: s->mu -> moq_endpoint_is_closed -> ep->mu). We hold ep->mu
     * and run the EXACT closed-check the BLOCK_TIMEOUT loop uses on a worker. It
     * must finish without taking ep->mu. Deterministic: ep->mu is held for the
     * whole window, so a path that took ep->mu would block for sure (never set
     * `done`); the correct path never touches ep->mu (sets `done` immediately). */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        MOQ_TEST_CHECK(s != NULL);
        moq_endpoint_t *ep = moq_endpoint_test_make_bare();
        MOQ_TEST_CHECK(ep != NULL);
        if (s && ep) {
            moq_media_sender_test_set_ep(s, ep);
            moq_endpoint_test_lock_mu(ep);   /* simulate the hook holding ep->mu */

            struct lockorder_arg arg;
            arg.s = s;
            atomic_init(&arg.done, false);
            pthread_t th;
            int prc = pthread_create(&th, NULL, lockorder_worker, &arg);
            MOQ_TEST_CHECK_EQ_INT(prc, 0);

            bool finished = false;
            for (int i = 0; i < 200 && !finished; i++) {   /* up to ~2s */
                if (atomic_load(&arg.done)) { finished = true; break; }
                struct timespec ts = { 0, 10 * 1000000L };   /* 10ms poll */
                nanosleep(&ts, NULL);
            }
            /* If the closed-check took ep->mu, the worker would block here. */
            MOQ_TEST_CHECK(finished);

            moq_endpoint_test_unlock_mu(ep);   /* release: unblocks a stuck worker */
            pthread_join(th, NULL);

            moq_media_sender_test_set_ep(s, NULL);   /* free must not touch ep */
        }
        if (s) moq_media_sender_test_free(s);
        if (ep) moq_endpoint_test_free_bare(ep);
        MOQ_TEST_PASS("sender_catalog.blocktimeout_no_ep_lock_inversion");
    }

    /* -- MSF §5.1.2 generatedAt: real wallclock ms, not the old `1` ----- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        add_trk(s, "v", false);   /* live track */
        moq_media_sender_test_set_ready(s);

        struct timespec tb, ta;
        clock_gettime(CLOCK_REALTIME, &tb);
        uint64_t before_ms = (uint64_t)tb.tv_sec * 1000u +
                             (uint64_t)tb.tv_nsec / 1000000u;
        moq_rcbuf_t *j = NULL;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_test_build_catalog(s, &j), (int)MOQ_OK);
        clock_gettime(CLOCK_REALTIME, &ta);
        uint64_t after_ms = (uint64_t)ta.tv_sec * 1000u +
                            (uint64_t)ta.tv_nsec / 1000000u;

        MOQ_TEST_CHECK(j != NULL);
        if (j) {
            const moq_alloc_t *al = moq_alloc_default();
            moq_msf_catalog_t c;
            if (moq_msf_catalog_parse(al,
                    (moq_bytes_t){ moq_rcbuf_data(j), moq_rcbuf_len(j) }, &c)
                == MOQ_OK) {
                MOQ_TEST_CHECK(c.has_generated_at);
                /* Captured during the build, so before <= generatedAt <= after. */
                MOQ_TEST_CHECK(c.generated_at >= before_ms &&
                               c.generated_at <= after_ms);
                MOQ_TEST_CHECK(c.generated_at != 1);   /* not the old sentinel */
                moq_msf_catalog_cleanup(al, &c);
            } else {
                MOQ_TEST_CHECK(0);
            }
            moq_rcbuf_decref(j);
        }
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.generated_at_wallclock");
    }

    /* -- CMSF §3.2: two CMAF switching-set tracks carry a common altGroup -- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        add_cmaf_altgroup(s, "hd", 7);
        add_cmaf_altgroup(s, "sd", 7);
        moq_media_sender_test_set_ready(s);

        bool ha = false; int av = 0;
        MOQ_TEST_CHECK(built_track_alt_group(s, "hd", &ha, &av));
        MOQ_TEST_CHECK(ha && av == 7);
        ha = false; av = 0;
        MOQ_TEST_CHECK(built_track_alt_group(s, "sd", &ha, &av));
        MOQ_TEST_CHECK(ha && av == 7);
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.cmsf_alt_group");
    }

    /* -- ABI: an older cfg prefix (no altGroup fields) defaults to absent -- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)"old", 3 };
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        tc.codec = (moq_bytes_t){ (const uint8_t *)"avc1.640028", 11 };
        tc.bitrate = 1500000;
        tc.timescale = 90000;
        tc.is_live = true;
        tc.has_alt_group = true;   /* set, but the old prefix excludes these bytes */
        tc.alt_group = 9;
        /* Simulate a caller built against the pre-altGroup struct: its
         * struct_size stops before has_alt_group, so the prefix-safe copy in
         * add_track must NOT read these trailing bytes. */
        tc.struct_size = offsetof(moq_media_track_cfg_t, has_alt_group);
        moq_media_track_t *t = NULL;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_add_track(s, &tc, &t), (int)MOQ_OK);
        moq_media_sender_test_set_ready(s);

        bool ha = true; int av = 99;
        MOQ_TEST_CHECK(built_track_alt_group(s, "old", &ha, &av));
        MOQ_TEST_CHECK(!ha);   /* altGroup absent: the old prefix never carried it */
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.alt_group_abi_old_prefix");
    }

    /* -- CMSF §4.1.1.4.5: pssh must be valid Base64 at create/attach -------
     * Mirrors the strict MSF catalog encoder so a bad contentProtection fails
     * the public authoring API (create/attach) rather than late at publish. */
    {
        moq_media_sender_cfg_t cfg;
        moq_media_sender_cfg_init_live(&cfg);   /* sets a valid backpressure */
        moq_bytes_t ns_part = { (const uint8_t *)"ns", 2 };
        cfg.namespace_.parts = &ns_part;
        cfg.namespace_.count = 1;

        moq_bytes_t kid =
            { (const uint8_t *)"01234567-89ab-cdef-0123-456789abcdef", 36 };
        moq_cmsf_content_protection_t cp;
        memset(&cp, 0, sizeof(cp));
        cp.ref_id = (moq_bytes_t){ (const uint8_t *)"cp1", 3 };
        cp.scheme = (moq_bytes_t){ (const uint8_t *)"cenc", 4 };
        cp.default_kids = &kid; cp.default_kid_count = 1;
        cp.drm_system.system_id =
            (moq_bytes_t){ (const uint8_t *)"edef8ba9-79d6-4ace-a3c8-27dcd51d21ed", 36 };
        cp.drm_system.has_pssh = true;
        cfg.content_protections = &cp;
        cfg.content_protection_count = 1;

        /* present-but-invalid Base64 pssh -> INVAL. */
        cp.drm_system.pssh = (moq_bytes_t){ (const uint8_t *)"not!base64", 10 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_test_validate_cfg(&cfg), (int)MOQ_ERR_INVAL);

        /* valid Base64 pssh -> accepted. */
        cp.drm_system.pssh = (moq_bytes_t){ (const uint8_t *)"AAAAAQ==", 8 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_test_validate_cfg(&cfg), (int)MOQ_OK);

        /* present-but-empty pssh -> INVAL (unchanged from before). */
        cp.drm_system.pssh = (moq_bytes_t){ NULL, 0 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_test_validate_cfg(&cfg), (int)MOQ_ERR_INVAL);

        /* sanity: no pssh at all is still fine. */
        cp.drm_system.has_pssh = false;
        cp.drm_system.pssh = (moq_bytes_t){ NULL, 0 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_test_validate_cfg(&cfg), (int)MOQ_OK);
        MOQ_TEST_PASS("sender_catalog.cmsf_pssh_base64");
    }

    /* -- Post-ready add: the staged generation must be strict MSF-01 ----- */
    /* Case: base catalog ready+published, one track added after ready, the
     * generation staged (E2E case D). The emitted objects are consumed by
     * OTHER MSF-01 receivers (PlayA), whose parsers are stricter than our
     * own lenient parse -- so assert the exact wire shape, not a libmoq
     * parse round-trip: base version String "1" (§5.1.1) and the §5.1.6
     * deltaUpdate array with no root version. */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        (void)add_trk(s, "v", false);
        moq_media_sender_test_set_ready(s);
        moq_media_sender_test_mark_registered(s);
        moq_media_sender_test_mark_published(s);   /* baseline = {v} */

        /* Post-ready add shaped like a real LOC video track (the E2E case D
         * shape): init data (inlined base64 into the delta add), width/
         * height/framerate. */
        {
            moq_media_track_cfg_t tc;
            moq_media_track_cfg_init(&tc);
            tc.name = (moq_bytes_t){ (const uint8_t *)"late", 4 };
            tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
            tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
            tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };
            tc.bitrate = 1500000;
            tc.is_live = true;
            static const uint8_t init_data[] = { 0x01, 0x42, 0xe0, 0x1e };
            tc.init_data = (moq_bytes_t){ init_data, sizeof(init_data) };
            tc.width = 320; tc.height = 240;
            tc.framerate_millis = 30000;
            moq_media_track_t *late = NULL;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_media_sender_add_track(s, &tc, &late), (int)MOQ_OK);
        }
        moq_media_sender_test_mark_registered(s);

        moq_rcbuf_t *objs[3] = { NULL, NULL, NULL };
        size_t nobj = moq_media_sender_test_stage(s, objs, 3);
        MOQ_TEST_CHECK_EQ_INT((int)nobj, 2);       /* base + one ADD delta */

        /* Object 0 -- the independent base. §5.1.1: version is the String
         * "1" (the exact value PlayA and the pre-rewrite libmoq agree on;
         * "draft-01" is rejected by strict MSF-01 receivers -- the E2E case D
         * regression). */
        {
            char tmp[512];
            size_t n = moq_rcbuf_len(objs[0]);
            MOQ_TEST_CHECK(n < sizeof(tmp));
            memcpy(tmp, moq_rcbuf_data(objs[0]), n); tmp[n] = '\0';
            MOQ_TEST_CHECK(strstr(tmp, "\"version\":\"1\"") != NULL);
            MOQ_TEST_CHECK(strstr(tmp, "draft-01") == NULL);
            MOQ_TEST_CHECK(strstr(tmp, "\"tracks\":[") != NULL);
        }

        /* Object 1 -- the delta. §5.1.6 shape: a deltaUpdate ARRAY of
         * {op, tracks} operations, and NO root version/tracks fields (a
         * delta MUST NOT carry them). The added track must be the full
         * self-contained shape (inline initData base64, no initRef). */
        {
            char tmp[1024];
            size_t n = moq_rcbuf_len(objs[1]);
            MOQ_TEST_CHECK(n < sizeof(tmp));
            memcpy(tmp, moq_rcbuf_data(objs[1]), n); tmp[n] = '\0';
            MOQ_TEST_CHECK(strncmp(tmp, "{\"deltaUpdate\":[{\"op\":\"add\"",
                                   27) == 0);
            MOQ_TEST_CHECK(strstr(tmp, "\"version\"") == NULL);
            MOQ_TEST_CHECK(strstr(tmp, "\"name\":\"late\"") != NULL);
            MOQ_TEST_CHECK(strstr(tmp, "\"initData\":\"AULgHg==\"") != NULL);
            MOQ_TEST_CHECK(strstr(tmp, "initRef") == NULL);
        }

        for (size_t i = 0; i < nobj; i++) moq_rcbuf_decref(objs[i]);
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("sender_catalog.post_ready_delta_strict_shape");
    }

    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
