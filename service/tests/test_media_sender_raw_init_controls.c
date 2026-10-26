/*
 * The CONTROLS for the init-bearing RAW/LOC record contract, which hold
 * TODAY, judged through the SAME oracle the RED rows use (raw_init_oracle.h):
 * a RAW add carrying no record is a legitimate delta; CMAF's referenced shape
 * is untouched; and the oracle rejects, by NAMED invariant, a dangling
 * reference, a legacy inline field (with bytes or empty), an unused extra
 * entry, and wrong bytes of the right length -- each perturbed on an OBSERVED
 * catalog, with the counts a count-based check would have accepted left
 * unchanged.
 *
 * Separate binary: the shared harness prints a row's PASS only while nothing
 * has failed, so a control cannot be shown green beside a deliberate RED.
 *
 * Fixture bytes are OPAQUE test records; nothing here claims decoder
 * semantics, and the CMAF control's "init segment" is such bytes too.
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

static const uint8_t RECORD[] = {
    0x01, 0x64, 0x00, 0x1f, 0xff, 0xe1, 0x00, 0x04, 0x67, 0x64, 0x00, 0x1f,
    0x01, 0x00, 0x04, 0x68, 0xee, 0x3c, 0x80
};
/* Same LENGTH, different bytes. */
static const uint8_t RECORD_SAME_LEN[] = {
    0x01, 0x4d, 0x40, 0x1e, 0xff, 0xe1, 0x00, 0x04, 0x67, 0x4d, 0x40, 0x1e,
    0x01, 0x00, 0x04, 0x68, 0xee, 0x3c, 0x80
};

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

static bool obj_is_delta(const moq_rcbuf_t *obj)
{
    moq_msf_catalog_t c;
    memset(&c, 0, sizeof(c));
    if (moq_msf_catalog_parse(moq_alloc_default(),
            (moq_bytes_t){ moq_rcbuf_data(obj), moq_rcbuf_len(obj) }, &c) != MOQ_OK)
        return false;
    bool delta = c.delta_update != NULL;
    moq_msf_catalog_cleanup(moq_alloc_default(), &c);
    return delta;
}

int main(void)
{
    /* -- c1: a RAW add with no record stays a delta -------------------- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        (void)add_raw_video(s, "v", RECORD, sizeof(RECORD));
        moq_media_sender_test_set_ready(s);
        moq_media_sender_test_mark_registered(s);
        moq_media_sender_test_mark_published(s);

        (void)add_raw_video(s, "plain", NULL, 0);
        moq_media_sender_test_mark_registered(s);

        moq_rcbuf_t *objs[4] = { NULL, NULL, NULL, NULL };
        size_t nobj = moq_media_sender_test_stage(s, objs, 4);
        MOQ_TEST_CHECK_EQ_U64(nobj, 2);            /* base + one ADD delta */
        if (nobj == 2) {
            MOQ_TEST_CHECK(!obj_is_delta(objs[0]));
            MOQ_TEST_CHECK(obj_is_delta(objs[1]));
        }
        for (size_t i = 0; i < nobj; i++) moq_rcbuf_decref(objs[i]);
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("raw_init.no_record_add_stays_a_delta");
    }

    /* -- c2: CMAF is unchanged, through the same oracle ---------------- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        (void)add_cmaf_video(s, "c", RECORD, sizeof(RECORD));
        moq_media_sender_test_set_ready(s);

        moq_rcbuf_t *json = NULL;
        moq_msf_catalog_t c;
        bool built = build_parsed(s, &json, &c);
        MOQ_TEST_CHECK(built);
        if (built) {
            MOQ_TEST_CHECK_EQ_U64(c.track_count, 1);
            MOQ_TEST_CHECK_EQ_U64(c.init_data_count, 1);
            MOQ_TEST_CHECK(raw_init_list_is_inline(&c));
            MOQ_TEST_CHECK(raw_init_no_legacy_field(&c));
            MOQ_TEST_CHECK(raw_init_list_is_exactly_used(&c));
            MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "c", RECORD, sizeof(RECORD)),
                                  (int)RAW_INIT_OK);
            drop_parsed(json, &c);
        }
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("raw_init.cmaf_shape_is_unchanged");
    }

    /* -- c3: the oracle rejects each perturbation of an OBSERVED catalog
     *    by named invariant, with counts a tally would accept unchanged -- */
    {
        moq_media_sender_t *s = moq_media_sender_test_new();
        (void)add_cmaf_video(s, "c", RECORD, sizeof(RECORD));
        moq_media_sender_test_set_ready(s);

        moq_rcbuf_t *json = NULL;
        moq_msf_catalog_t c;
        bool built = build_parsed(s, &json, &c);
        MOQ_TEST_CHECK(built);
        if (built) {
            moq_msf_track_t *t = (moq_msf_track_t *)raw_init_track(&c, "c");
            MOQ_TEST_CHECK(t != NULL);
            if (t) {
                const size_t entries = c.init_data_count;
                const size_t tracks = c.track_count;
                const moq_bytes_t ref = t->init_ref;

                /* Wrong bytes of the right length. */
                MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "c", RECORD_SAME_LEN,
                                                            sizeof(RECORD_SAME_LEN)),
                                      (int)RAW_INIT_WRONG_BYTES);

                /* The observed reference re-pointed at nothing declared. */
                t->init_ref = (moq_bytes_t){ (const uint8_t *)"nosuch", 6 };
                MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "c", RECORD, sizeof(RECORD)),
                                      (int)RAW_INIT_DANGLING_REF);
                MOQ_TEST_CHECK(!raw_init_list_is_exactly_used(&c));
                MOQ_TEST_CHECK_EQ_U64(c.init_data_count, entries);   /* a tally sees nothing */
                MOQ_TEST_CHECK_EQ_U64(c.track_count, tracks);
                t->init_ref = ref;

                /* The reference removed altogether. */
                t->has_init_ref = false;
                MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "c", RECORD, sizeof(RECORD)),
                                      (int)RAW_INIT_NO_REF);
                MOQ_TEST_CHECK(!raw_init_list_is_exactly_used(&c));   /* the entry is now unused */
                t->has_init_ref = true;

                /* A legacy inline field, present and EMPTY. */
                t->has_init_data = true;
                t->init_data = (moq_bytes_t){ NULL, 0 };
                MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "c", RECORD, sizeof(RECORD)),
                                      (int)RAW_INIT_LEGACY_INLINE);
                MOQ_TEST_CHECK(!raw_init_no_legacy_field(&c));
                t->has_init_data = false;

                /* An extra entry nothing references: the SAME resolution for
                 * "c", yet no longer exactly used. */
                {
                    moq_msf_init_data_entry_t widened[2];
                    memset(widened, 0, sizeof(widened));
                    widened[0] = c.init_data_list[0];
                    widened[1] = c.init_data_list[0];
                    widened[1].id = (moq_bytes_t){ (const uint8_t *)"orphan", 6 };
                    moq_msf_init_data_entry_t *saved_list = c.init_data_list;
                    size_t saved_count = c.init_data_count;
                    c.init_data_list = widened;
                    c.init_data_count = 2;
                    MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "c", RECORD, sizeof(RECORD)),
                                          (int)RAW_INIT_OK);
                    MOQ_TEST_CHECK(!raw_init_list_is_exactly_used(&c));
                    c.init_data_list = saved_list;
                    c.init_data_count = saved_count;
                }

                /* A track that does not exist. */
                MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "absent", RECORD, sizeof(RECORD)),
                                      (int)RAW_INIT_NO_TRACK);

                /* Restored: the observed catalog still resolves. */
                MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "c", RECORD, sizeof(RECORD)),
                                      (int)RAW_INIT_OK);
                MOQ_TEST_CHECK(raw_init_list_is_exactly_used(&c));
            }
            drop_parsed(json, &c);
        }
        moq_media_sender_test_free(s);
        MOQ_TEST_PASS("raw_init.oracle_rejects_each_perturbation_by_name");
    }

    return failures == 0 ? 0 : 1;
}
