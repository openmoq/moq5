/*
 * Initialization reference IDENTITY across generations.
 *
 * MSF-01 §5.3 fixes a declared Namespace|Name tuple's attributes; §5.2.13
 * makes initRef one of them; §5.1.7 gives list ids catalog scope. So a track
 * that survives a later generation keeps the initRef it was declared with,
 * whatever happened to the track that introduced the shared entry. The
 * receiver compares init_ref as part of the immutable tuple
 * (media_receiver.c:1040); what is measured here is the PRODUCER's output,
 * not a live receiver rejection.
 *
 * Both rows are built on the seams the accepted RAW rows use, and each seam
 * simulates one committed state: mark_registered = the publisher tracks are
 * registered; mark_published = the sender committed the current resolved
 * catalog as its baseline, as it does after a wire send; test_stage = the
 * wire object list sender_stage_generation produced, without sending it.
 * Records are OPAQUE test bytes; the shared oracle judges references and
 * bytes only.
 *
 * White box, deterministic, no network.
 */
#include <moq/media_sender.h>
#include <moq/msf.h>
#include <moq/rcbuf.h>
#include "test_support.h"
#include "raw_init_oracle.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

moq_media_sender_t *moq_media_sender_test_new(void);
void moq_media_sender_test_free(moq_media_sender_t *s);
void moq_media_sender_test_set_ready(moq_media_sender_t *s);
moq_result_t moq_media_sender_test_build_catalog(moq_media_sender_t *s,
                                                 moq_rcbuf_t **out);
void moq_media_sender_test_mark_published(moq_media_sender_t *s);
void moq_media_sender_test_mark_registered(moq_media_sender_t *s);
size_t moq_media_sender_test_stage(moq_media_sender_t *s,
                                   moq_rcbuf_t **objs, size_t cap);
bool moq_media_sender_test_build_changed(moq_media_sender_t *s);
moq_result_t moq_media_sender_test_new_cfg_alloc_limit(
    const moq_media_sender_cfg_t *cfg, const moq_alloc_t *alloc,
    size_t size_limit, moq_media_sender_t **out);
void moq_media_sender_test_set_init_entry_next(moq_media_sender_t *s, uint64_t v);
void moq_media_sender_test_script_generated_at(moq_media_sender_t *s, uint64_t first, uint64_t rest);
uint64_t moq_media_sender_test_init_entry_next(moq_media_sender_t *s);

/* A counting allocator that refuses exactly ONE armed request, by index,
 * and records what it refused. */
typedef struct {
    long   allocs;          /* requests so far (alloc + realloc) */
    long   live;
    long   fail_at;         /* -1 = never */
    long   refused_index;
    size_t refused_size;
} refuser_t;

static void *r_alloc(size_t size, void *ctx)
{
    refuser_t *r = (refuser_t *)ctx;
    long idx = r->allocs++;
    if (idx == r->fail_at) { r->refused_index = idx; r->refused_size = size; return NULL; }
    void *p = malloc(size ? size : 1);
    if (p) r->live++;
    return p;
}
static void *r_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    refuser_t *r = (refuser_t *)ctx;
    (void)old_size;
    long idx = r->allocs++;
    if (idx == r->fail_at) { r->refused_index = idx; r->refused_size = new_size; return NULL; }
    bool fresh = (ptr == NULL);   /* decided BEFORE the old block may be gone */
    void *p = realloc(ptr, new_size ? new_size : 1);
    if (p && fresh) r->live++;
    return p;
}
static void r_free(void *ptr, size_t size, void *ctx)
{
    refuser_t *r = (refuser_t *)ctx;
    (void)size;
    if (ptr) { free(ptr); r->live--; }
}

static moq_media_sender_t *new_refusing_sender(refuser_t *r, moq_bytes_t *parts)
{
    memset(r, 0, sizeof(*r));
    r->fail_at = -1;
    r->refused_index = -1;
    moq_alloc_t a = { r, r_alloc, r_realloc, r_free };
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    parts[0] = MOQ_BYTES_LITERAL("svc");
    parts[1] = MOQ_BYTES_LITERAL("id");
    cfg.namespace_ = (moq_namespace_t){ parts, 2 };
    cfg.backpressure = MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK;
    moq_media_sender_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_new_cfg_alloc_limit(
        &cfg, &a, SIZE_MAX, &s), (int)MOQ_OK);
    return s;
}

static const uint8_t VIDEO_RECORD[] = {
    0x01, 0x64, 0x00, 0x1f, 0xff, 0xe1, 0x00, 0x04, 0x67, 0x64, 0x00, 0x1f,
    0x01, 0x00, 0x04, 0x68, 0xee, 0x3c, 0x80
};
static const uint8_t AUDIO_ASC[] = { 0x11, 0x90 };
static const uint8_t AUDIO_ASC_2[] = { 0x12, 0x10 };   /* a distinct record */

static bool ref_is(const moq_msf_catalog_t *c, const char *name, const char *id)
{
    raw_init_ref_t r = raw_init_ref_of(c, name);
    return r.ok && r.len == strlen(id) && memcmp(r.id, id, r.len) == 0;
}

static moq_media_track_t *add_raw_video(moq_media_sender_t *s, const char *name,
                                        const uint8_t *init, size_t init_len)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"avc1.64001f", 11 };
    tc.bitrate = 1500000;
    tc.is_live = true;
    tc.width = 640;
    tc.height = 480;
    if (init_len > 0) tc.init_data = (moq_bytes_t){ init, init_len };
    moq_media_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                          (int)MOQ_OK);
    return t;
}

static moq_media_track_t *add_raw_audio(moq_media_sender_t *s, const char *name,
                                        const uint8_t *init, size_t init_len)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"mp4a.40.2", 9 };
    tc.bitrate = 128000;
    tc.is_live = true;
    tc.samplerate = 48000;
    tc.channel_config = (moq_bytes_t){ (const uint8_t *)"2", 1 };
    tc.init_data = (moq_bytes_t){ init, init_len };
    moq_media_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                          (int)MOQ_OK);
    return t;
}

static moq_media_track_t *add_cmaf_video(moq_media_sender_t *s, const char *name,
                                         const uint8_t *init, size_t init_len)
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
    tc.init_data = (moq_bytes_t){ init, init_len };
    moq_media_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                          (int)MOQ_OK);
    return t;
}

static bool build_parsed(moq_media_sender_t *s, moq_rcbuf_t **json,
                         moq_msf_catalog_t *out)
{
    *json = NULL;
    if (moq_media_sender_test_build_catalog(s, json) != MOQ_OK || !*json)
        return false;
    if (moq_msf_catalog_parse(moq_alloc_default(),
            (moq_bytes_t){ moq_rcbuf_data(*json), moq_rcbuf_len(*json) },
            out) != MOQ_OK) {
        moq_rcbuf_decref(*json);
        *json = NULL;
        return false;
    }
    return true;
}

static void drop_parsed(moq_rcbuf_t *json, moq_msf_catalog_t *c)
{
    moq_msf_catalog_cleanup(moq_alloc_default(), c);
    if (json) moq_rcbuf_decref(json);
}

static bool parse_obj(const moq_rcbuf_t *obj, moq_msf_catalog_t *out)
{
    memset(out, 0, sizeof(*out));
    return moq_msf_catalog_parse(moq_alloc_default(),
        (moq_bytes_t){ moq_rcbuf_data(obj), moq_rcbuf_len(obj) }, out) == MOQ_OK;
}

/* The committed baseline {v1, v2} sharing ONE entry; returns v1's handle and
 * copies out the reference v2 was DECLARED with. */
static moq_media_track_t *shared_baseline(moq_media_sender_t *s,
                                          raw_init_ref_t *declared_v2)
{
    moq_media_track_t *v1 = add_raw_video(s, "v1", VIDEO_RECORD, sizeof(VIDEO_RECORD));
    (void)add_raw_video(s, "v2", VIDEO_RECORD, sizeof(VIDEO_RECORD));
    moq_media_sender_test_set_ready(s);
    moq_media_sender_test_mark_registered(s);
    moq_rcbuf_t *json = NULL;
    moq_msf_catalog_t c;
    memset(declared_v2, 0, sizeof(*declared_v2));
    bool built = build_parsed(s, &json, &c);
    MOQ_TEST_CHECK(built);
    if (built) {
        MOQ_TEST_CHECK_EQ_U64(c.track_count, 2);
        MOQ_TEST_CHECK_EQ_U64(c.init_data_count, 1);
        MOQ_TEST_CHECK(raw_init_same_ref(raw_init_track(&c, "v1"),
                                         raw_init_track(&c, "v2")));
        *declared_v2 = raw_init_ref_of(&c, "v2");
        drop_parsed(json, &c);
    }
    MOQ_TEST_CHECK(declared_v2->ok);
    moq_media_sender_test_mark_published(s);   /* the baseline is committed */
    return v1;
}

int main(void)
{
    /* -- i1: the introducing track is removed; a survivor keeps its ref --
     * remove v1 (the entry's introducer), add an init-bearing audio track:
     * one independent generation whose v2 still carries the reference v2
     * was declared with, resolving to the same exact bytes. */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        raw_init_ref_t declared;
        moq_media_track_t *v1 = shared_baseline(s, &declared);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, v1), (int)MOQ_OK);
        (void)add_raw_audio(s, "a", AUDIO_ASC, sizeof(AUDIO_ASC));
        moq_media_sender_test_mark_registered(s);

        moq_rcbuf_t *objs[4] = { NULL, NULL, NULL, NULL };
        size_t n = moq_media_sender_test_stage(s, objs, 4);
        MOQ_TEST_CHECK_EQ_U64(n, 1);               /* an init-bearing add: independent */
        if (n >= 1) {
            moq_msf_catalog_t c;
            bool parsed = parse_obj(objs[0], &c);
            MOQ_TEST_CHECK(parsed);
            if (parsed) {
                MOQ_TEST_CHECK(c.delta_update == NULL);
                MOQ_TEST_CHECK_EQ_U64(c.track_count, 2);      /* v2, a */
                MOQ_TEST_CHECK(raw_init_track(&c, "v1") == NULL);
                MOQ_TEST_CHECK_EQ_U64(c.init_data_count, 2);  /* v2's, a's */
                MOQ_TEST_CHECK(raw_init_list_is_inline(&c));
                MOQ_TEST_CHECK(raw_init_no_legacy_field(&c));
                MOQ_TEST_CHECK(raw_init_list_is_exactly_used(&c));
                MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "v2", VIDEO_RECORD,
                                                            sizeof(VIDEO_RECORD)),
                                      (int)RAW_INIT_OK);
                MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "a", AUDIO_ASC,
                                                            sizeof(AUDIO_ASC)),
                                      (int)RAW_INIT_OK);
                /* THE INVARIANT: the survivor's declared attribute is unchanged. */
                MOQ_TEST_CHECK(raw_init_ref_eq(declared, raw_init_ref_of(&c, "v2")));
                moq_msf_catalog_cleanup(moq_alloc_default(), &c);
            }
        }
        for (size_t i = 0; i < n; i++) moq_rcbuf_decref(objs[i]);
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("raw_init_identity.survivor_keeps_its_reference_after_introducer_removal");
    }

    /* -- i2: remove-only, then a later unrelated no-record add ----------
     * Generation A is the committed base plus one REMOVE delta: v2's
     * reference is inherited from the base untouched, and the base's entry
     * legitimately remains (the invariant is stable references, not the
     * absence of an unreferenced entry in an inherited base). Generation B,
     * a no-record add, is a NEW independent base plus one ADD delta; that
     * base is where a producer that rebuilds ids from scratch changes v2's
     * attribute. Both effectives are rebuilt from the actual staged objects. */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        raw_init_ref_t declared;
        moq_media_track_t *v1 = shared_baseline(s, &declared);

        /* Generation A: remove only. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, v1), (int)MOQ_OK);
        {
            moq_rcbuf_t *objs[4] = { NULL, NULL, NULL, NULL };
            size_t n = moq_media_sender_test_stage(s, objs, 4);
            MOQ_TEST_CHECK_EQ_U64(n, 2);           /* committed base + REMOVE delta */
            if (n == 2) {
                moq_msf_catalog_t base, delta, eff;
                memset(&eff, 0, sizeof(eff));
                bool pb = parse_obj(objs[0], &base);
                bool pd = pb && parse_obj(objs[1], &delta);
                MOQ_TEST_CHECK(pb && pd);
                if (pb && pd) {
                    MOQ_TEST_CHECK(base.delta_update == NULL);
                    MOQ_TEST_CHECK_EQ_U64(base.track_count, 2);
                    MOQ_TEST_CHECK_EQ_U64(base.init_data_count, 1);
                    MOQ_TEST_CHECK(delta.delta_update != NULL);
                    MOQ_TEST_CHECK_EQ_U64(delta.delta_update_count, 1);
                    if (delta.delta_update_count == 1) {
                        MOQ_TEST_CHECK_EQ_INT((int)delta.delta_update[0].op,
                                              (int)MOQ_MSF_DELTA_OP_REMOVE);
                        MOQ_TEST_CHECK_EQ_U64(delta.delta_update[0].track_count, 1);
                    }
                    bool applied = moq_msf_catalog_apply_delta(
                        moq_alloc_default(), &base, &delta, &eff) == MOQ_OK;
                    MOQ_TEST_CHECK(applied);
                    if (applied) {
                        MOQ_TEST_CHECK_EQ_U64(eff.track_count, 1);
                        MOQ_TEST_CHECK(raw_init_track(&eff, "v1") == NULL);
                        /* The inherited base's one entry stays; v2 still names it. */
                        MOQ_TEST_CHECK_EQ_U64(eff.init_data_count, 1);
                        MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&eff, "v2", VIDEO_RECORD,
                                                                    sizeof(VIDEO_RECORD)),
                                              (int)RAW_INIT_OK);
                        MOQ_TEST_CHECK(raw_init_ref_eq(declared, raw_init_ref_of(&eff, "v2")));
                        moq_msf_catalog_cleanup(moq_alloc_default(), &eff);
                    }
                }
                if (pd) moq_msf_catalog_cleanup(moq_alloc_default(), &delta);
                if (pb) moq_msf_catalog_cleanup(moq_alloc_default(), &base);
            }
            for (size_t i = 0; i < n; i++) moq_rcbuf_decref(objs[i]);
        }
        moq_media_sender_test_mark_published(s);   /* generation A committed */

        /* Generation B: an unrelated no-record add. */
        (void)add_raw_video(s, "plain", NULL, 0);
        moq_media_sender_test_mark_registered(s);
        {
            moq_rcbuf_t *objs[4] = { NULL, NULL, NULL, NULL };
            size_t n = moq_media_sender_test_stage(s, objs, 4);
            MOQ_TEST_CHECK_EQ_U64(n, 2);           /* new base + ADD delta */
            if (n == 2) {
                moq_msf_catalog_t base, delta, eff;
                memset(&eff, 0, sizeof(eff));
                bool pb = parse_obj(objs[0], &base);
                bool pd = pb && parse_obj(objs[1], &delta);
                MOQ_TEST_CHECK(pb && pd);
                if (pb && pd) {
                    MOQ_TEST_CHECK(base.delta_update == NULL);
                    MOQ_TEST_CHECK_EQ_U64(base.track_count, 1);        /* v2 */
                    MOQ_TEST_CHECK_EQ_U64(base.init_data_count, 1);
                    MOQ_TEST_CHECK(raw_init_list_is_exactly_used(&base));
                    MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&base, "v2", VIDEO_RECORD,
                                                                sizeof(VIDEO_RECORD)),
                                          (int)RAW_INIT_OK);
                    /* THE INVARIANT, on the base the survivor is re-declared in. */
                    MOQ_TEST_CHECK(raw_init_ref_eq(declared, raw_init_ref_of(&base, "v2")));
                    MOQ_TEST_CHECK(delta.delta_update != NULL);
                    MOQ_TEST_CHECK_EQ_U64(delta.delta_update_count, 1);
                    MOQ_TEST_CHECK_EQ_U64(delta.track_count, 0);
                    if (delta.delta_update_count == 1) {   /* one ADD of bare plain */
                        MOQ_TEST_CHECK_EQ_INT((int)delta.delta_update[0].op,
                                              (int)MOQ_MSF_DELTA_OP_ADD);
                        MOQ_TEST_CHECK_EQ_U64(delta.delta_update[0].track_count, 1);
                        if (delta.delta_update[0].track_count == 1) {
                            const moq_msf_track_t *d = &delta.delta_update[0].tracks[0];
                            MOQ_TEST_CHECK(d->name.len == 5 &&
                                           memcmp(d->name.data, "plain", 5) == 0);
                            MOQ_TEST_CHECK(!d->has_init_ref && !d->has_init_data);
                        }
                    }
                    bool applied = moq_msf_catalog_apply_delta(
                        moq_alloc_default(), &base, &delta, &eff) == MOQ_OK;
                    MOQ_TEST_CHECK(applied);
                    if (applied) {
                        MOQ_TEST_CHECK_EQ_U64(eff.track_count, 2);     /* v2, plain */
                        MOQ_TEST_CHECK(raw_init_ref_eq(declared, raw_init_ref_of(&eff, "v2")));
                        const moq_msf_track_t *p = raw_init_track(&eff, "plain");
                        MOQ_TEST_CHECK(p != NULL);
                        if (p) MOQ_TEST_CHECK(!p->has_init_ref && !p->has_init_data);
                        moq_msf_catalog_cleanup(moq_alloc_default(), &eff);
                    }
                }
                if (pd) moq_msf_catalog_cleanup(moq_alloc_default(), &delta);
                if (pb) moq_msf_catalog_cleanup(moq_alloc_default(), &base);
            }
            for (size_t i = 0; i < n; i++) moq_rcbuf_decref(objs[i]);
        }
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("raw_init_identity.remove_then_unrelated_add_keeps_the_reference");
    }


    /* -- s1/s2: equal records share one entry across RAW and CMAF, in both
     * introduction orders, each resolving byte-exact. */
    for (int order = 0; order < 2; order++) {
        moq_media_sender_t *s = moq_media_sender_test_new();
        if (order == 0) {
            (void)add_raw_video(s, "r", VIDEO_RECORD, sizeof(VIDEO_RECORD));
            (void)add_cmaf_video(s, "c", VIDEO_RECORD, sizeof(VIDEO_RECORD));
        } else {
            (void)add_cmaf_video(s, "c", VIDEO_RECORD, sizeof(VIDEO_RECORD));
            (void)add_raw_video(s, "r", VIDEO_RECORD, sizeof(VIDEO_RECORD));
        }
        moq_media_sender_test_set_ready(s);
        moq_rcbuf_t *json = NULL;
        moq_msf_catalog_t c;
        bool built = build_parsed(s, &json, &c);
        MOQ_TEST_CHECK(built);
        if (built) {
            MOQ_TEST_CHECK_EQ_U64(c.track_count, 2);
            MOQ_TEST_CHECK_EQ_U64(c.init_data_count, 1);
            MOQ_TEST_CHECK(raw_init_list_is_exactly_used(&c));
            MOQ_TEST_CHECK(raw_init_same_ref(raw_init_track(&c, "r"), raw_init_track(&c, "c")));
            MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "r", VIDEO_RECORD, sizeof(VIDEO_RECORD)),
                                  (int)RAW_INIT_OK);
            MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "c", VIDEO_RECORD, sizeof(VIDEO_RECORD)),
                                  (int)RAW_INIT_OK);
            drop_parsed(json, &c);
        }
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS(order == 0 ? "raw_init_identity.raw_then_cmaf_share_one_entry"
                                 : "raw_init_identity.cmaf_then_raw_share_one_entry");
    }

    /* -- s3: repeated builds and content probes preserve identities, and
     * the committed no-op dedup still holds -- with generatedAt SCRIPTED.
     * Equal content and stable identities never require equal wall-clock
     * stamps, so whole-JSON equality is asserted only under one declared
     * stamp; under declared DIFFERENT stamps each build is parsed and its
     * stamp, identities and exact record bytes are asserted instead. Every
     * object test_stage transfers is released on every path, and the
     * sender's allocator is the counting one so balance is explicit. */
    {
        refuser_t r;
        moq_bytes_t parts[2];
        moq_media_sender_t *s = new_refusing_sender(&r, parts);
        (void)add_raw_video(s, "v", VIDEO_RECORD, sizeof(VIDEO_RECORD));
        (void)add_raw_audio(s, "a", AUDIO_ASC, sizeof(AUDIO_ASC));
        moq_media_sender_test_set_ready(s);

        /* One declared stamp on both builds: byte-identical. */
        moq_media_sender_test_script_generated_at(s, 1700000000123ull, 1700000000123ull);
        moq_rcbuf_t *j1 = NULL, *j2 = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &j1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &j2), (int)MOQ_OK);
        MOQ_TEST_CHECK(j1 && j2 && moq_rcbuf_len(j1) == moq_rcbuf_len(j2) &&
                       memcmp(moq_rcbuf_data(j1), moq_rcbuf_data(j2), moq_rcbuf_len(j1)) == 0);
        if (j1) moq_rcbuf_decref(j1);
        if (j2) moq_rcbuf_decref(j2);

        /* Declared DIFFERENT stamps: the stamp moves, nothing else does. */
        static const uint64_t STAMPS[2] = { 1700000000456ull, 1700000000789ull };
        for (int k = 0; k < 2; k++) {
            moq_media_sender_test_script_generated_at(s, STAMPS[k], STAMPS[k]);
            moq_rcbuf_t *j = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &j), (int)MOQ_OK);
            moq_msf_catalog_t c;
            bool parsed = j && parse_obj(j, &c);
            MOQ_TEST_CHECK(parsed);
            if (parsed) {
                MOQ_TEST_CHECK(c.has_generated_at);
                MOQ_TEST_CHECK_EQ_U64(c.generated_at, STAMPS[k]);
                MOQ_TEST_CHECK_EQ_U64(c.track_count, 2);
                MOQ_TEST_CHECK_EQ_U64(c.init_data_count, 2);
                MOQ_TEST_CHECK(ref_is(&c, "v", "1"));   /* the first two identities, in order */
                MOQ_TEST_CHECK(ref_is(&c, "a", "2"));
                MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "v", VIDEO_RECORD, sizeof(VIDEO_RECORD)),
                                      (int)RAW_INIT_OK);
                MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "a", AUDIO_ASC, sizeof(AUDIO_ASC)),
                                      (int)RAW_INIT_OK);
                moq_msf_catalog_cleanup(moq_alloc_default(), &c);
            }
            if (j) moq_rcbuf_decref(j);
        }

        /* Committed, then a LATER stamp request that would differ: the
         * content dedup still says nothing changed and nothing is staged. */
        moq_media_sender_test_mark_registered(s);
        moq_media_sender_test_mark_published(s);
        moq_media_sender_test_script_generated_at(s, 1700000001000ull, 1700000001000ull);
        MOQ_TEST_CHECK(!moq_media_sender_test_build_changed(s));   /* no-op dedup */
        moq_rcbuf_t *objs[4] = { NULL, NULL, NULL, NULL };
        size_t staged = moq_media_sender_test_stage(s, objs, 4);
        MOQ_TEST_CHECK_EQ_U64(staged, 0);
        /* Whatever was transferred is released, assertion or not. */
        for (size_t i = 0; i < staged && i < 4; i++) if (objs[i]) moq_rcbuf_decref(objs[i]);
        moq_media_sender_test_free(s);
        fprintf(stderr, "s3 allocator balance: live=%ld staged=%zu\n", r.live, staged);
        MOQ_TEST_CHECK_EQ_INT((int)r.live, 0);
        MOQ_TEST_PASS("raw_init_identity.repeat_builds_keep_identities_and_dedup");
    }

    /* -- s4: a deterministic allocation refusal AFTER the identity was
     * reserved cleans up and cannot change that identity later. The first
     * build's LAST request is refused (the encoded catalog itself, after the
     * reservation inside the builder); the next build succeeds under the
     * same identity "1", so the failed build neither rolled it back nor
     * reserved a second one. */
    {
        refuser_t count;
        moq_bytes_t parts[2];
        long per_build;
        {   /* measure the request count of one successful build */
            moq_media_sender_t *s = new_refusing_sender(&count, parts);
            (void)add_raw_video(s, "v", VIDEO_RECORD, sizeof(VIDEO_RECORD));
            moq_media_sender_test_set_ready(s);
            long before = count.allocs;
            moq_rcbuf_t *j = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &j), (int)MOQ_OK);
            per_build = count.allocs - before;
            if (j) moq_rcbuf_decref(j);
            moq_media_sender_test_free(s);
            MOQ_TEST_CHECK(per_build >= 2);
            MOQ_TEST_CHECK_EQ_INT((int)count.live, 0);
        }
        refuser_t r;
        moq_media_sender_t *s = new_refusing_sender(&r, parts);
        (void)add_raw_video(s, "v", VIDEO_RECORD, sizeof(VIDEO_RECORD));
        moq_media_sender_test_set_ready(s);
        long live0 = r.live;
        r.fail_at = r.allocs + per_build - 1;          /* the build's last request */
        moq_rcbuf_t *j = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &j), (int)MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(j == NULL);
        MOQ_TEST_CHECK_EQ_INT((int)(r.refused_index - (r.fail_at - per_build + 1)), (int)(per_build - 1));
        MOQ_TEST_CHECK(r.refused_size > 0);
        MOQ_TEST_CHECK_EQ_INT((int)r.live, (int)live0);   /* every temporary released */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_init_entry_next(s), 1); /* reserved, kept */
        r.fail_at = -1;
        moq_rcbuf_t *j2 = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &j2), (int)MOQ_OK);
        moq_msf_catalog_t c;
        bool parsed = j2 && parse_obj(j2, &c);
        MOQ_TEST_CHECK(parsed);
        if (parsed) {
            MOQ_TEST_CHECK(ref_is(&c, "v", "1"));
            MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "v", VIDEO_RECORD, sizeof(VIDEO_RECORD)),
                                  (int)RAW_INIT_OK);
            moq_msf_catalog_cleanup(moq_alloc_default(), &c);
        }
        if (j2) moq_rcbuf_decref(j2);
        /* A later equal record still adopts that identity. */
        (void)add_raw_video(s, "w", VIDEO_RECORD, sizeof(VIDEO_RECORD));
        moq_rcbuf_t *j3 = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &j3), (int)MOQ_OK);
        parsed = j3 && parse_obj(j3, &c);
        MOQ_TEST_CHECK(parsed);
        if (parsed) {
            MOQ_TEST_CHECK_EQ_U64(c.init_data_count, 1);
            MOQ_TEST_CHECK(ref_is(&c, "w", "1"));
            moq_msf_catalog_cleanup(moq_alloc_default(), &c);
        }
        if (j3) moq_rcbuf_decref(j3);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_init_entry_next(s), 1);
        moq_media_sender_test_free(s);
        MOQ_TEST_CHECK_EQ_INT((int)r.live, 0);
        MOQ_TEST_PASS("raw_init_identity.refused_encode_after_reservation_keeps_the_identity");
    }

    /* -- s5: the counter's boundary -- an arithmetic self-check through a
     * test seam, not a claim that a uint64 lifetime is reachable. At the
     * last usable value a distinct new record is refused without wrapping;
     * assigned tracks and donors keep working. */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        moq_media_sender_test_set_init_entry_next(s, UINT64_MAX - 1);
        (void)add_raw_video(s, "v", VIDEO_RECORD, sizeof(VIDEO_RECORD));
        moq_media_sender_test_set_ready(s);
        moq_rcbuf_t *json = NULL;
        moq_msf_catalog_t c;
        bool built = build_parsed(s, &json, &c);
        MOQ_TEST_CHECK(built);
        if (built) {
            MOQ_TEST_CHECK(ref_is(&c, "v", "18446744073709551615"));   /* the final value */
            drop_parsed(json, &c);
        }
        MOQ_TEST_CHECK(moq_media_sender_test_init_entry_next(s) == UINT64_MAX);
        /* A donor still serves an equal record. */
        (void)add_raw_video(s, "w", VIDEO_RECORD, sizeof(VIDEO_RECORD));
        built = build_parsed(s, &json, &c);
        MOQ_TEST_CHECK(built);
        if (built) {
            MOQ_TEST_CHECK_EQ_U64(c.init_data_count, 1);
            MOQ_TEST_CHECK(ref_is(&c, "w", "18446744073709551615"));
            drop_parsed(json, &c);
        }
        /* A DISTINCT record has no identity left: refused, no wrap. */
        moq_media_track_t *z = add_raw_audio(s, "z", AUDIO_ASC_2, sizeof(AUDIO_ASC_2));
        moq_rcbuf_t *jz = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &jz), (int)MOQ_ERR_UNSUPPORTED);
        MOQ_TEST_CHECK(jz == NULL);
        MOQ_TEST_CHECK(moq_media_sender_test_init_entry_next(s) == UINT64_MAX);
        /* Without it, the assigned tracks still build under their identity. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, z), (int)MOQ_OK);
        built = build_parsed(s, &json, &c);
        MOQ_TEST_CHECK(built);
        if (built) {
            MOQ_TEST_CHECK_EQ_U64(c.track_count, 2);
            MOQ_TEST_CHECK(ref_is(&c, "v", "18446744073709551615"));
            MOQ_TEST_CHECK(ref_is(&c, "w", "18446744073709551615"));
            drop_parsed(json, &c);
        }
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("raw_init_identity.counter_boundary_refuses_without_wrapping");
    }

    return failures == 0 ? 0 : 1;
}
