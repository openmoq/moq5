/*
 * moq_locmaf_write_cmaf(): the canonical CMAF chunk of section 15, byte for
 * byte against the golden fixtures; the size probe; the consistency checks
 * on a caller-built chunk; and the written chunk read back by the CMAF
 * parser and validator.
 */
#include "locmaf_fixtures.h"

#include <moq/cmaf.h>

static void test_fixture_bytes(void)
{
    for (size_t s = 0; s < lm_sequence_count; s++) {
        const lm_sequence_t *seq = &lm_sequences[s];
        moq_locmaf_track_t track = lm_fixture_track(&seq->items[0]);
        lm_dec_t dec;
        CHECK_RC(lm_dec_open(&dec, &track, 0, 0, 0), MOQ_OK);
        for (size_t i = 0; i < seq->count; i++) {
            const lm_fixture_t *f = &seq->items[i];
            uint8_t obj[512], want[512], got[512];
            size_t on = lm_fixture_object(f, obj, sizeof(obj));
            size_t wn = lm_fixture_canonical(f, want, sizeof(want));
            size_t gn = 0;
            moq_locmaf_reason_t why;
            moq_result_t rc = lm_decode_write(&dec, &track, 0, f->object_id, obj, on, got,
                                              sizeof(got), &gn, &why);
            if (rc != MOQ_OK) fprintf(stderr, "  fixture %s (%s)\n", f->name,
                                      moq_locmaf_reason_name(why));
            CHECK_RC(rc, MOQ_OK);
            if (rc == MOQ_OK) CHECK_BYTES(got, gn, want, wn);
        }
        lm_dec_close(&dec);
    }
}

/* A decoded chunk to mutate: F4 (n 3, varying sizes) or F9 (CENC). */
typedef struct held {
    lm_dec_t           dec;
    moq_locmaf_track_t track;
    moq_locmaf_chunk_t chunk;
    uint8_t            obj[512];
    moq_locmaf_sample_t samples[8];
    moq_locmaf_subsample_t subs[8];
    moq_locmaf_gen_box_t boxes[4];
} held_t;

static void hold(held_t *h, size_t seq)
{
    const lm_fixture_t *f = &lm_sequences[seq].items[0];
    h->track = lm_fixture_track(f);
    CHECK_RC(lm_dec_open(&h->dec, &h->track, 0, 0, 0), MOQ_OK);
    size_t n = lm_fixture_object(f, h->obj, sizeof(h->obj));
    moq_locmaf_reason_t why;
    memset(&h->chunk, 0, sizeof(h->chunk));
    moq_result_t rc = moq_locmaf_decoder_push(&h->dec.d, 0, 0, (moq_bytes_t){ h->obj, n },
                                              &h->chunk, sizeof(h->chunk), &why);
    CHECK_RC(rc, MOQ_OK);
    if (rc != MOQ_OK || h->chunk.sample_count > 8 || h->chunk.subsample_total > 8 ||
        h->chunk.gen_box_count > 4) {
        fprintf(stderr, "hold: fixture did not decode\n");
        exit(1);
    }
    /* Private copies of the tables, so a mutation leaves the decoder alone. */
    memcpy(h->samples, h->chunk.samples, h->chunk.sample_count * sizeof(h->samples[0]));
    h->chunk.samples = h->samples;
    if (h->chunk.subsample_total) {
        memcpy(h->subs, h->chunk.subsamples, h->chunk.subsample_total * sizeof(h->subs[0]));
        h->chunk.subsamples = h->subs;
    }
    if (h->chunk.gen_box_count) {
        memcpy(h->boxes, h->chunk.gen_boxes, h->chunk.gen_box_count * sizeof(h->boxes[0]));
        h->chunk.gen_boxes = h->boxes;
    }
}

static size_t write_len(const moq_locmaf_track_t *t, const moq_locmaf_chunk_t *c,
                        moq_result_t *rc)
{
    size_t len = 0x5555;
    *rc = moq_locmaf_write_cmaf(t, c, NULL, 0, &len);
    return len;
}

static void test_probe(void)
{
    held_t h;
    hold(&h, 2);                       /* F4 */
    moq_result_t rc;
    size_t need = write_len(&h.track, &h.chunk, &rc);
    CHECK_RC(rc, MOQ_ERR_BUFFER);
    CHECK_EQ(need, 100 + 8 + 15);
    uint8_t buf[256];
    if (need >= sizeof(buf)) { lm_dec_close(&h.dec); return; }
    memset(buf, 0xA5, sizeof(buf));
    size_t len = 0;
    CHECK_RC(moq_locmaf_write_cmaf(&h.track, &h.chunk, buf, need - 1, &len), MOQ_ERR_BUFFER);
    CHECK_EQ(len, need);
    bool untouched = true;
    for (size_t i = 0; i < sizeof(buf); i++) untouched &= buf[i] == 0xA5;
    CHECK(untouched);
    CHECK_RC(moq_locmaf_write_cmaf(&h.track, &h.chunk, buf, need, &len), MOQ_OK);
    CHECK_EQ(len, need);
    CHECK(buf[need] == 0xA5);
    CHECK_RC(moq_locmaf_write_cmaf(&h.track, &h.chunk, buf, sizeof(buf), &len), MOQ_OK);
    CHECK_EQ(len, need);
    /* arguments */
    CHECK_RC(moq_locmaf_write_cmaf(NULL, &h.chunk, buf, sizeof(buf), &len), MOQ_ERR_INVAL);
    CHECK_RC(moq_locmaf_write_cmaf(&h.track, NULL, buf, sizeof(buf), &len), MOQ_ERR_INVAL);
    CHECK_RC(moq_locmaf_write_cmaf(&h.track, &h.chunk, buf, sizeof(buf), NULL), MOQ_ERR_INVAL);
    CHECK_RC(moq_locmaf_write_cmaf(&h.track, &h.chunk, NULL, 8, &len), MOQ_ERR_INVAL);
    lm_dec_close(&h.dec);

    /* rawBoxes: one CMAF chunk, verbatim */
    {
        moq_locmaf_chunk_t c;
        memset(&c, 0, sizeof(c));
        c.struct_size = (uint32_t)sizeof(c);
        c.kind = MOQ_LOCMAF_OBJECT_RAW_BOXES;
        uint8_t boxes[256];
        size_t bn = lm_fixture_canonical(&lm_sequences[0].items[0], boxes, sizeof(boxes));
        c.raw_boxes = (moq_bytes_t){ boxes, bn };
        moq_locmaf_track_t t = lm_track_a();
        CHECK_RC(moq_locmaf_write_cmaf(&t, &c, buf, sizeof(buf), &len), MOQ_OK);
        CHECK_BYTES(buf, len, boxes, bn);
        c.raw_boxes = (moq_bytes_t){ NULL, 0 };
        CHECK_RC(moq_locmaf_write_cmaf(&t, &c, buf, sizeof(buf), &len), MOQ_ERR_INVAL);
    }
}

/* A caller-built rawBoxes record is held to the rule a decoded one met
 * (sections 9.1, 18): complete boxes with plain 32-bit sizes summing to the
 * bytes, exactly one CMAF chunk, every box valid for its type. */
static void test_raw_boxes_refused(void)
{
    static const char *bad[] = {
        "00000007 66726565",                                 /* size below 8 */
        "00000000 66726565",                                 /* size 0 */
        "00000001 66726565 0000000000000010",                /* largesize */
        "00000008 66726565 0000",                            /* short tail */
        "00000009 66726565",                                 /* past the end */
        "00000008 66726565",                                 /* no chunk */
        "00000010 66747970 636D6663 00000000 00000008 6D6F6F76", /* ftyp + moov */
    };
    moq_locmaf_track_t t = lm_track_a();
    uint8_t boxes[1024], out[1024];
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        size_t bn = lm_hex(bad[i], boxes, sizeof(boxes));
        moq_locmaf_chunk_t c;
        memset(&c, 0, sizeof(c));
        c.struct_size = (uint32_t)sizeof(c);
        c.kind = MOQ_LOCMAF_OBJECT_RAW_BOXES;
        c.raw_boxes = (moq_bytes_t){ boxes, bn };
        size_t len = 0x5555;
        moq_result_t rc = moq_locmaf_write_cmaf(&t, &c, out, sizeof(out), &len);
        if (rc != MOQ_ERR_INVAL) fprintf(stderr, "  rawBoxes: %s\n", bad[i]);
        CHECK_RC(rc, MOQ_ERR_INVAL);
        CHECK_EQ(len, 0x5555);
    }

    /* Chunk shapes: build F1's canonical chunk, then break one box. */
    const lm_fixture_t *f1 = &lm_sequences[0].items[0];
    for (int shape = 0; shape < 7; shape++) {
        size_t bn = lm_fixture_canonical(f1, boxes, sizeof(boxes));
        switch (shape) {
        case 0:                                   /* two chunks */
            memcpy(boxes + bn, boxes, bn);
            bn *= 2;
            break;
        case 1:                                   /* tfhd with an unknown flag */
            boxes[32 + 11] |= 0x40;
            break;
        case 2:                                   /* trun sizes flag, no records */
            boxes[72 + 10] |= 0x02;
            break;
        case 3: {                                 /* a box after the mdat */
            static const uint8_t fr[8] = { 0, 0, 0, 8, 'f', 'r', 'e', 'e' };
            memcpy(boxes + bn, fr, 8);
            bn += 8;
            break;
        }
        case 4: {                                 /* a second styp ahead of the moof */
            static const uint8_t st[32] = {
                0, 0, 0, 16, 's', 't', 'y', 'p', 'c', 'm', 'f', 's', 0, 0, 0, 0,
                0, 0, 0, 16, 's', 't', 'y', 'p', 'c', 'm', 'f', 's', 0, 0, 0, 0 };
            memmove(boxes + 32, boxes, bn);
            memcpy(boxes, st, 32);
            bn += 32;
            break;
        }
        case 5:                                   /* traf child renamed 'meta' */
            memcpy(boxes + 52 + 4, "meta", 4);
            break;
        default:                                  /* mfhd size 1 (an escape) */
            boxes[8 + 3] = 1;
            break;
        }
        moq_locmaf_chunk_t c;
        memset(&c, 0, sizeof(c));
        c.struct_size = (uint32_t)sizeof(c);
        c.kind = MOQ_LOCMAF_OBJECT_RAW_BOXES;
        c.raw_boxes = (moq_bytes_t){ boxes, bn };
        size_t len = 0x5555;
        moq_result_t rc = moq_locmaf_write_cmaf(&t, &c, out, sizeof(out), &len);
        if (rc != MOQ_ERR_INVAL) fprintf(stderr, "  rawBoxes shape %d\n", shape);
        CHECK_RC(rc, MOQ_ERR_INVAL);
        CHECK_EQ(len, 0x5555);
    }
}

typedef void (*mutate_fn)(held_t *h);

static void expect_inval(size_t seq, mutate_fn m, const char *what)
{
    held_t h;
    hold(&h, seq);
    m(&h);
    uint8_t buf[512];
    size_t len = 0x5555;
    moq_result_t rc = moq_locmaf_write_cmaf(&h.track, &h.chunk, buf, sizeof(buf), &len);
    if (rc != MOQ_ERR_INVAL) fprintf(stderr, "  consistency: %s\n", what);
    CHECK_RC(rc, MOQ_ERR_INVAL);
    CHECK_EQ(len, 0x5555);
    lm_dec_close(&h.dec);
}

static void m_struct_size(held_t *h) { h->chunk.struct_size = MOQ_LOCMAF_CHUNK_V0_SIZE - 1; }
static void m_track_size(held_t *h) { h->track.struct_size = MOQ_LOCMAF_TRACK_V0_SIZE - 1; }
static void m_kind(held_t *h) { h->chunk.kind = MOQ_LOCMAF_OBJECT_NONE; }
static void m_kind_unknown(held_t *h) { h->chunk.kind = 9; }
static void m_size_sum(held_t *h) { h->samples[1].size++; }
static void m_offset(held_t *h) { h->samples[2].offset++; }
static void m_samples_null(held_t *h) { h->chunk.samples = NULL; }
static void m_event_payload(held_t *h) { h->chunk.sample_count = 0; }
static void m_cto_range(held_t *h) { h->samples[0].composition_offset = (int64_t)UINT32_MAX + 1; }
static void m_cto_low(held_t *h) { h->samples[0].composition_offset = (int64_t)INT32_MIN - 1; }
static void m_cto_joint(held_t *h)
{
    h->samples[0].composition_offset = -1;
    h->samples[1].composition_offset = (int64_t)INT32_MAX + 1;
}
static void m_aux_clear(held_t *h) { h->chunk.has_aux_info = true; h->chunk.per_sample_iv_size = 8; }
static void m_mdat_null(held_t *h) { h->chunk.mdat_payload.data = NULL; }
static void m_gen_null(held_t *h) { h->chunk.gen_box_count = 1; h->chunk.gen_boxes = NULL; }
static void m_gen_payload(held_t *h)
{
    h->boxes[0].type = LM_FOURCC_T('f','r','e','e');
    h->boxes[0].payload = (moq_bytes_t){ NULL, 3 };
    h->chunk.gen_boxes = h->boxes;
    h->chunk.gen_box_count = 1;
}
static void m_subs_count(held_t *h) { h->samples[0].subsample_count = 1; }
/* F9: protected, IV 8, subsamples [1, 2] */
static void m_ivs_len(held_t *h) { h->chunk.ivs.len--; }
static void m_iv_size(held_t *h) { h->chunk.per_sample_iv_size = 12; }
static void m_sub_index(held_t *h) { h->samples[1].subsample_index = 0; }
static void m_sub_total(held_t *h) { h->chunk.subsample_total--; }
static void m_sub_sum(held_t *h) { h->subs[0].clear_bytes++; }
static void m_clear_wide(held_t *h)
{
    h->subs[0].clear_bytes = 65536;
    h->subs[0].protected_bytes = 100 - 65536u;  /* wraps: the sum no longer matches */
}
static void m_sub_without_aux(held_t *h) { h->chunk.has_aux_info = false; }
static void m_aux_unprotected(held_t *h) { h->track.is_protected = false; }
static void m_subs_null(held_t *h) { h->chunk.subsamples = NULL; }
static void m_flag_off(held_t *h)
{
    /* subsample counts without the subsample flag */
    h->chunk.has_subsamples = false;
}
/* F9 with its auxiliary information removed: IV size 8 on a protected
 * track, no IVs, no map. */
static void m_no_aux(held_t *h)
{
    h->chunk.has_aux_info = false;
    h->chunk.has_subsamples = false;
    h->chunk.ivs = (moq_bytes_t){ NULL, 0 };
    h->chunk.subsamples = NULL;
    h->chunk.subsample_total = 0;
    for (uint32_t i = 0; i < h->chunk.sample_count; i++) {
        h->samples[i].subsample_count = 0;
        h->samples[i].subsample_index = 0;
    }
}
static void m_no_aux_16(held_t *h)
{
    m_no_aux(h);
    h->track = lm_track_p16();
    h->chunk.per_sample_iv_size = 16;
}
/* legal: the same chunk with IV size 0 (field 16 lowered it) */
static void m_no_aux_size0(held_t *h)
{
    m_no_aux(h);
    h->chunk.per_sample_iv_size = 0;
}

static void expect_written(size_t seq, mutate_fn m, const char *what)
{
    held_t h;
    hold(&h, seq);
    if (m) m(&h);
    uint8_t buf[512];
    size_t len = 0;
    moq_result_t rc = moq_locmaf_write_cmaf(&h.track, &h.chunk, buf, sizeof(buf), &len);
    if (rc != MOQ_OK) fprintf(stderr, "  written: %s\n", what);
    CHECK_RC(rc, MOQ_OK);
    lm_dec_close(&h.dec);
}

static void test_consistency(void)
{
    expect_inval(2, m_struct_size, "chunk below its extent");
    expect_inval(2, m_track_size, "track below its extent");
    expect_inval(2, m_kind, "kind none");
    expect_inval(2, m_kind_unknown, "kind unknown");
    expect_inval(2, m_size_sum, "sizes not summing to the payload");
    expect_inval(2, m_offset, "an offset off the running sum");
    expect_inval(2, m_samples_null, "no sample table");
    expect_inval(2, m_event_payload, "no samples with a payload");
    expect_inval(2, m_cto_range, "an offset past uint32");
    expect_inval(2, m_cto_low, "an offset below int32");
    expect_inval(2, m_cto_joint, "offsets no trun version holds");
    expect_inval(2, m_aux_clear, "aux info on an unprotected track");
    expect_inval(2, m_mdat_null, "payload bytes missing");
    expect_inval(2, m_gen_null, "genBox table missing");
    expect_inval(2, m_gen_payload, "genBox payload bytes missing");
    expect_inval(2, m_subs_count, "subsample counts without a map");
    expect_inval(7, m_ivs_len, "IVs of the wrong length");
    expect_inval(7, m_iv_size, "an IV size a track cannot have");
    expect_inval(7, m_sub_index, "a subsample index off the running sum");
    expect_inval(7, m_sub_total, "a subsample total off the counts");
    expect_inval(7, m_sub_sum, "subsamples not summing to the sample");
    expect_inval(7, m_clear_wide, "clear bytes past 16 bits");
    expect_inval(7, m_sub_without_aux, "subsamples without aux info");
    expect_inval(7, m_aux_unprotected, "aux info on a track without protection");
    expect_inval(7, m_subs_null, "subsample table missing");
    expect_inval(7, m_flag_off, "subsample counts with the flag clear");
    /* sections 13.1, 15.8: a protected chunk with a nonzero IV size carries
     * its auxiliary information */
    expect_inval(7, m_no_aux, "IV size 8 with no IVs");
    expect_inval(7, m_no_aux_16, "IV size 16 with no IVs");
    expect_written(7, m_no_aux_size0, "IV size 0 with no IVs");
    expect_written(9, NULL, "a constant-IV track's chunk without CENC fields");
}

/* parse_fragment ignores trex: a value the canonical chunk leaves to trex
 * reads back as 0. */
static bool same_or_trex(uint32_t parsed, uint32_t effective, uint32_t trex)
{
    return parsed == effective || (parsed == 0 && effective == trex);
}

static void test_cross_parse(void)
{
    for (size_t s = 0; s < lm_sequence_count; s++) {
        const lm_sequence_t *seq = &lm_sequences[s];
        moq_locmaf_track_t track = lm_fixture_track(&seq->items[0]);
        lm_dec_t dec;
        CHECK_RC(lm_dec_open(&dec, &track, 0, 0, 0), MOQ_OK);
        for (size_t i = 0; i < seq->count; i++) {
            const lm_fixture_t *f = &seq->items[i];
            uint8_t obj[512], out[512];
            size_t on = lm_fixture_object(f, obj, sizeof(obj));
            moq_locmaf_chunk_t ch;
            moq_locmaf_reason_t why;
            memset(&ch, 0, sizeof(ch));
            moq_result_t rc = moq_locmaf_decoder_push(&dec.d, 0, f->object_id,
                (moq_bytes_t){ obj, on }, &ch, sizeof(ch), &why);
            CHECK_RC(rc, MOQ_OK);
            size_t len = 0;
            if (rc != MOQ_OK || ch.gen_box_count > 4) continue;
            CHECK_RC(moq_locmaf_write_cmaf(&track, &ch, out, sizeof(out), &len), MOQ_OK);
            /* The CMAF parser skips only some pre-moof boxes; read the
             * chunk from its moof. */
            size_t at = 0;
            for (size_t g = 0; g < ch.gen_box_count; g++) at += 8 + ch.gen_boxes[g].payload.len;
            CHECK(at < len);
            moq_cmaf_sample_t ps[8];
            moq_cmaf_fragment_info_t frag;
            moq_cmaf_fragment_info_init(&frag, ps, 8);
            CHECK_RC(moq_cmaf_parse_fragment((moq_bytes_t){ out + at, len - at }, &frag), MOQ_OK);
            CHECK_EQ(frag.sample_count, ch.sample_count);
            CHECK_EQ(frag.track_id, track.track_id);
            CHECK_EQ(frag.base_decode_time, ch.base_media_decode_time);
            CHECK_EQ(frag.chunk_count, 1);
            CHECK(frag.mdat.len == ch.mdat_payload.len);
            for (size_t k = 0; k < frag.sample_count && k < 8; k++) {
                CHECK(same_or_trex(ps[k].duration, ch.samples[k].duration,
                                   track.trex_default_sample_duration));
                CHECK(same_or_trex(ps[k].size, ch.samples[k].size,
                                   track.trex_default_sample_size));
                CHECK(same_or_trex(ps[k].flags, ch.samples[k].flags,
                                   track.trex_default_sample_flags));
                CHECK_EQ((int64_t)ps[k].composition_offset, ch.samples[k].composition_offset);
            }
            moq_cmaf_object_report_t rep;
            moq_cmaf_init_info_t init;
            memset(&init, 0, sizeof(init));
            init.struct_size = (uint32_t)MOQ_CMAF_INIT_INFO_V0_SIZE;
            init.track_id = track.track_id;
            moq_result_t vrc = moq_cmaf_validate_object(&init, (moq_bytes_t){ out + at, len - at },
                                                        &rep);
            if (ch.sample_count >= 1) {
                CHECK_RC(vrc, MOQ_OK);
                CHECK(rep.valid && rep.chunk_count == 1);
            } else {
                CHECK_RC(vrc, MOQ_ERR_PROTO);
                CHECK(rep.reason == MOQ_CMAF_ERR_NO_SAMPLES);
            }
        }
        lm_dec_close(&dec);
    }
}

int main(void)
{
    test_fixture_bytes();
    test_probe();
    test_consistency();
    test_raw_boxes_refused();
    test_cross_parse();
    return lm_finish("locmaf_write");
}
