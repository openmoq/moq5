/*
 * The producer contract for an init-bearing RAW/LOC track's decoder record.
 *
 * MSF-01 carries initialization data in the root Initialization Data List
 * (§5.1.7), referenced from a track by initRef (§5.2.13); MSF-01 defines no
 * per-track initData field. This sender's RULED producer policy for an
 * init-bearing RAW add after the first catalog is the same fallback CMAF
 * already takes: a full independent generation, so the reference and its
 * data stay self-contained and stable across generations. That is a chosen
 * producer contract, not a claim that the protocol forbids a delta from
 * referencing data an effective base already holds; no delta-list semantics
 * are designed here.
 *
 * These rows judge the catalog BYTES the sender produces, through the shared
 * oracle in raw_init_oracle.h. Our own receiver also tolerates a legacy
 * per-track inline initData; that tolerance is compatibility and is never
 * the oracle.
 *
 * Fixture bytes are OPAQUE test records: the oracle compares bytes and
 * claims nothing about decoder semantics. The ASC is the independently
 * derived AudioSpecificConfig for AAC-LC, 48 kHz, 2 channels (AOT 2 = 00010,
 * frequency index 3 = 0011, channel configuration 2 = 0010, then 000 ->
 * 0x11 0x90), so it agrees with the declared samplerate; the AVC bytes are
 * not a decodable configuration and are not claimed to be one.
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
#include <string.h>

static int failures = 0;

/* Test seam (media_sender.c, MOQ_MEDIA_SENDER_TESTING). */
moq_media_sender_t *moq_media_sender_test_new(void);
void moq_media_sender_test_free(moq_media_sender_t *s);
void moq_media_sender_test_set_ready(moq_media_sender_t *s);
moq_result_t moq_media_sender_test_build_catalog(moq_media_sender_t *s,
                                                 moq_rcbuf_t **out);
void moq_media_sender_test_mark_published(moq_media_sender_t *s);
void moq_media_sender_test_mark_registered(moq_media_sender_t *s);
size_t moq_media_sender_test_stage(moq_media_sender_t *s,
                                   moq_rcbuf_t **objs, size_t cap);

/* -- fixtures -------------------------------------------------------- */

static const uint8_t VIDEO_RECORD[] = {
    0x01, 0x64, 0x00, 0x1f, 0xff, 0xe1, 0x00, 0x04, 0x67, 0x64, 0x00, 0x1f,
    0x01, 0x00, 0x04, 0x68, 0xee, 0x3c, 0x80
};
static const uint8_t AUDIO_ASC[] = { 0x11, 0x90 };   /* AAC-LC 48 kHz stereo */

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
    tc.framerate_millis = 30000;
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
    if (init_len > 0) tc.init_data = (moq_bytes_t){ init, init_len };
    moq_media_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                          (int)MOQ_OK);
    return t;
}

/* Build the independent catalog and parse it. On true the caller owns both. */
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

/* Parse one staged object. On true the caller owns *out. */
static bool parse_obj(const moq_rcbuf_t *obj, moq_msf_catalog_t *out)
{
    memset(out, 0, sizeof(*out));
    return moq_msf_catalog_parse(moq_alloc_default(),
        (moq_bytes_t){ moq_rcbuf_data(obj), moq_rcbuf_len(obj) }, out) == MOQ_OK;
}

/* The declared inventory of a catalog that carries exactly {v: VIDEO_RECORD}
 * and {a-or-late: AUDIO_ASC} as its records, and `extra` tracks with none. */
static void expect_two_records(const moq_msf_catalog_t *c, const char *audio,
                               size_t track_count)
{
    MOQ_TEST_CHECK_EQ_U64(c->track_count, track_count);
    MOQ_TEST_CHECK_EQ_U64(c->init_data_count, 2);
    MOQ_TEST_CHECK(raw_init_list_is_inline(c));
    MOQ_TEST_CHECK(raw_init_no_legacy_field(c));
    MOQ_TEST_CHECK(raw_init_list_is_exactly_used(c));
    MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(c, "v", VIDEO_RECORD,
                                                sizeof(VIDEO_RECORD)),
                          (int)RAW_INIT_OK);
    MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(c, audio, AUDIO_ASC,
                                                sizeof(AUDIO_ASC)),
                          (int)RAW_INIT_OK);
    MOQ_TEST_CHECK(!raw_init_same_ref(raw_init_track(c, "v"),
                                      raw_init_track(c, audio)));
}

int main(void)
{
    /* -- r1: both initial RAW records are referenced, not inlined ------- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        MOQ_TEST_CHECK(s != NULL);
        (void)add_raw_video(s, "v", VIDEO_RECORD, sizeof(VIDEO_RECORD));
        (void)add_raw_audio(s, "a", AUDIO_ASC, sizeof(AUDIO_ASC));
        moq_media_sender_test_set_ready(s);

        moq_rcbuf_t *json = NULL;
        moq_msf_catalog_t c;
        bool built = build_parsed(s, &json, &c);
        MOQ_TEST_CHECK(built);
        if (built) {
            expect_two_records(&c, "a", 2);
            /* The metadata each record belongs to, exactly. */
            const moq_msf_track_t *tv = raw_init_track(&c, "v");
            const moq_msf_track_t *ta = raw_init_track(&c, "a");
            MOQ_TEST_CHECK(tv != NULL && ta != NULL);
            if (tv && ta) {
                MOQ_TEST_CHECK(tv->codec.len == 11 &&
                    memcmp(tv->codec.data, "avc1.64001f", 11) == 0);
                MOQ_TEST_CHECK_EQ_U64(tv->width, 640);
                MOQ_TEST_CHECK_EQ_U64(tv->height, 480);
                MOQ_TEST_CHECK(ta->codec.len == 9 &&
                    memcmp(ta->codec.data, "mp4a.40.2", 9) == 0);
                MOQ_TEST_CHECK_EQ_U64(ta->samplerate, 48000);
                MOQ_TEST_CHECK(ta->channel_config.len == 1 &&
                    ta->channel_config.data[0] == '2');
            }
            drop_parsed(json, &c);
        }
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("raw_init.initial_records_are_referenced");
    }

    /* -- r2: byte-identical records share ONE entry, both resolving ---- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        (void)add_raw_video(s, "v1", VIDEO_RECORD, sizeof(VIDEO_RECORD));
        (void)add_raw_video(s, "v2", VIDEO_RECORD, sizeof(VIDEO_RECORD));
        moq_media_sender_test_set_ready(s);

        moq_rcbuf_t *json = NULL;
        moq_msf_catalog_t c;
        bool built = build_parsed(s, &json, &c);
        MOQ_TEST_CHECK(built);
        if (built) {
            MOQ_TEST_CHECK_EQ_U64(c.track_count, 2);
            MOQ_TEST_CHECK_EQ_U64(c.init_data_count, 1);
            MOQ_TEST_CHECK(raw_init_list_is_inline(&c));
            MOQ_TEST_CHECK(raw_init_no_legacy_field(&c));
            MOQ_TEST_CHECK(raw_init_list_is_exactly_used(&c));
            /* BOTH resolve -- sharing is not one track silently losing its
             * record -- and they name the SAME entry. */
            MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "v1", VIDEO_RECORD,
                                                        sizeof(VIDEO_RECORD)),
                                  (int)RAW_INIT_OK);
            MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "v2", VIDEO_RECORD,
                                                        sizeof(VIDEO_RECORD)),
                                  (int)RAW_INIT_OK);
            MOQ_TEST_CHECK(raw_init_same_ref(raw_init_track(&c, "v1"),
                                             raw_init_track(&c, "v2")));
            drop_parsed(json, &c);
        }
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("raw_init.identical_records_share_one_entry");
    }

    /* -- r3: an init-bearing RAW late add is an INDEPENDENT generation -- */
    /* The ruled producer policy: the same independent-generation fallback
     * CMAF takes, so the reference is self-contained in the object that
     * carries it. A candidate that only fixed the initial catalog still
     * stages base + ADD delta here and fails this row. */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        (void)add_raw_video(s, "v", VIDEO_RECORD, sizeof(VIDEO_RECORD));
        moq_media_sender_test_set_ready(s);
        moq_media_sender_test_mark_registered(s);
        moq_media_sender_test_mark_published(s);   /* baseline = {v} */

        (void)add_raw_audio(s, "late", AUDIO_ASC, sizeof(AUDIO_ASC));
        moq_media_sender_test_mark_registered(s);

        moq_rcbuf_t *objs[4] = { NULL, NULL, NULL, NULL };
        size_t nobj = moq_media_sender_test_stage(s, objs, 4);
        MOQ_TEST_CHECK_EQ_U64(nobj, 1);            /* one independent generation */
        if (nobj >= 1) {
            moq_msf_catalog_t c;
            bool parsed = parse_obj(objs[0], &c);
            MOQ_TEST_CHECK(parsed);
            if (parsed) {
                MOQ_TEST_CHECK(c.delta_update == NULL);   /* structurally independent */
                MOQ_TEST_CHECK_EQ_U64(c.delta_update_count, 0);
                expect_two_records(&c, "late", 2);
                moq_msf_catalog_cleanup(moq_alloc_default(), &c);
            }
        }
        for (size_t i = 0; i < nobj; i++) moq_rcbuf_decref(objs[i]);
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("raw_init.late_add_is_an_independent_generation");
    }

    /* -- r4: continuity across the ACTUAL staged generations ------------ */
    /* The effective catalog is rebuilt from what was STAGED -- the
     * independent base and the ordered deltas, applied through the product's
     * own moq_msf_catalog_apply_delta -- never from a fresh full build alone.
     * MSF-01 §5.3 makes a declared track's attributes immutable: the
     * committed reference identities and records of the two earlier tracks
     * survive an unrelated no-record add, and that add carries none. */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        (void)add_raw_video(s, "v", VIDEO_RECORD, sizeof(VIDEO_RECORD));
        moq_media_sender_test_set_ready(s);
        moq_media_sender_test_mark_registered(s);
        moq_media_sender_test_mark_published(s);

        /* Generation A: the init-bearing late add. Its committed identities
         * are what generation B must preserve. */
        raw_init_ref_t ref_v_a, ref_late_a;
        memset(&ref_v_a, 0, sizeof(ref_v_a));
        memset(&ref_late_a, 0, sizeof(ref_late_a));
        (void)add_raw_audio(s, "late", AUDIO_ASC, sizeof(AUDIO_ASC));
        moq_media_sender_test_mark_registered(s);
        {
            moq_rcbuf_t *objs[4] = { NULL, NULL, NULL, NULL };
            size_t n = moq_media_sender_test_stage(s, objs, 4);
            MOQ_TEST_CHECK_EQ_U64(n, 1);
            if (n >= 1) {
                moq_msf_catalog_t a;
                bool parsed = parse_obj(objs[0], &a);
                MOQ_TEST_CHECK(parsed);
                if (parsed) {
                    ref_v_a = raw_init_ref_of(&a, "v");
                    ref_late_a = raw_init_ref_of(&a, "late");
                    moq_msf_catalog_cleanup(moq_alloc_default(), &a);
                }
            }
            for (size_t i = 0; i < n; i++) moq_rcbuf_decref(objs[i]);
        }
        MOQ_TEST_CHECK(ref_v_a.ok && ref_late_a.ok);
        moq_media_sender_test_mark_published(s);   /* generation A committed */

        /* Generation B: an unrelated add, carrying no record. Staged as the
         * committed base plus one ADD delta. */
        (void)add_raw_video(s, "plain", NULL, 0);
        moq_media_sender_test_mark_registered(s);
        {
            moq_rcbuf_t *objs[4] = { NULL, NULL, NULL, NULL };
            size_t n = moq_media_sender_test_stage(s, objs, 4);
            MOQ_TEST_CHECK_EQ_U64(n, 2);
            if (n == 2) {
                moq_msf_catalog_t base, delta, eff;
                memset(&eff, 0, sizeof(eff));
                bool pb = parse_obj(objs[0], &base);
                bool pd = pb && parse_obj(objs[1], &delta);
                MOQ_TEST_CHECK(pb && pd);
                if (pb && pd) {
                    /* Object inventory: an independent base of the two
                     * committed tracks, then ONE add op of ONE track. */
                    MOQ_TEST_CHECK(base.delta_update == NULL);
                    MOQ_TEST_CHECK_EQ_U64(base.track_count, 2);
                    MOQ_TEST_CHECK(delta.delta_update != NULL);
                    MOQ_TEST_CHECK_EQ_U64(delta.delta_update_count, 1);
                    MOQ_TEST_CHECK_EQ_U64(delta.track_count, 0);
                    if (delta.delta_update_count == 1) {
                        MOQ_TEST_CHECK_EQ_INT((int)delta.delta_update[0].op,
                                              (int)MOQ_MSF_DELTA_OP_ADD);
                        MOQ_TEST_CHECK_EQ_U64(delta.delta_update[0].track_count, 1);
                        if (delta.delta_update[0].track_count == 1) {
                            const moq_msf_track_t *p = &delta.delta_update[0].tracks[0];
                            MOQ_TEST_CHECK(p->name.len == 5 &&
                                           memcmp(p->name.data, "plain", 5) == 0);
                            MOQ_TEST_CHECK(!p->has_init_ref && !p->has_init_data);
                        }
                    }
                    /* The effective catalog, through the product's own apply. */
                    bool applied = moq_msf_catalog_apply_delta(
                        moq_alloc_default(), &base, &delta, &eff) == MOQ_OK;
                    MOQ_TEST_CHECK(applied);
                    if (applied) {
                        expect_two_records(&eff, "late", 3);
                        MOQ_TEST_CHECK(raw_init_ref_eq(ref_v_a,
                                                       raw_init_ref_of(&eff, "v")));
                        MOQ_TEST_CHECK(raw_init_ref_eq(ref_late_a,
                                                       raw_init_ref_of(&eff, "late")));
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

        /* An additional check only: a fresh full build agrees with what was
         * staged. It is not the authority. */
        {
            moq_rcbuf_t *json = NULL;
            moq_msf_catalog_t c;
            bool built = build_parsed(s, &json, &c);
            MOQ_TEST_CHECK(built);
            if (built) {
                expect_two_records(&c, "late", 3);
                MOQ_TEST_CHECK(raw_init_ref_eq(ref_v_a, raw_init_ref_of(&c, "v")));
                MOQ_TEST_CHECK(raw_init_ref_eq(ref_late_a, raw_init_ref_of(&c, "late")));
                drop_parsed(json, &c);
            }
        }
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("raw_init.continuity_across_staged_generations");
    }

    return failures == 0 ? 0 : 1;
}
