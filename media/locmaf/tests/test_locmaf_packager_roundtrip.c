/*
 * Chunks from the single-track CMAF writer, for every codec it writes:
 * header facts from its CMAF Header, then each chunk through encode, decode
 * and the canonical writer, equal to the chunk canonicalized alone, with the
 * CMAF parser reading the same sample table from the source and from the
 * canonical chunk.
 */
#include "locmaf_fixtures.h"

#include <moq/cmaf.h>
#include <moq/cmaf_packager.h>

static const uint8_t avcc[] = {
    1, 0x64, 0, 0x1f, 0xff, 0xe1, 0, 4, 0x67, 0x64, 0, 0x1f, 1, 0, 2, 0x68, 0xee
};
static const uint8_t hvcc[] = {
    1, 1, 0x60, 0, 0, 0, 0x90, 0, 0, 0, 0, 0, 0x5d, 0xf0, 0, 0xfc,
    0xfd, 0xf8, 0xf8, 0, 0, 0x0f, 3,
    0xa0, 0, 1, 0, 2, 0x40, 1,
    0xa1, 0, 1, 0, 2, 0x42, 1,
    0xa2, 0, 1, 0, 2, 0x44, 1
};
static const uint8_t av1c[] = { 0x81, 0, 0x0c, 0 };
static const uint8_t asc[] = { 0x11, 0x90 };
static const uint8_t dops[] = { 0, 2, 1, 0x38, 0, 0, 0xbb, 0x80, 0, 0, 0 };
static const uint8_t nal_key[] = { 0, 0, 1, 0x65, 0x11, 0x22 };
static const uint8_t nal_p[] = { 0, 0, 1, 0x41, 0x33 };
static const uint8_t hevc_key[] = { 0, 0, 1, 0x26, 1, 0x11 };
static const uint8_t hevc_p[] = { 0, 0, 1, 0x02, 1, 0x22, 0x33 };
static const uint8_t blob_a[] = { 0x12, 0x34, 0x56 };
static const uint8_t blob_b[] = { 0x78, 0x9A };
static const uint8_t obu[] = { 0x12, 0x00, 0x32, 0x01, 0xAA };
static const uint8_t opus[] = { 0xFC, 0xFF, 0xFE };

typedef struct codec {
    const char *name;
    moq_cmaf_sample_entry_t entry;
    moq_cmaf_sample_format_t format;
    const uint8_t *config;
    size_t config_len;
    uint32_t timescale;
    const uint8_t *key, *other;
    size_t key_len, other_len;
    uint32_t duration;
} codec_t;

static const codec_t codecs[] = {
    { "avc1", MOQ_CMAF_ENTRY_AVC1, MOQ_CMAF_SAMPLE_NAL_ANNEXB, avcc, sizeof(avcc), 90000,
      nal_key, nal_p, sizeof(nal_key), sizeof(nal_p), 3000 },
    { "hvc1", MOQ_CMAF_ENTRY_HVC1, MOQ_CMAF_SAMPLE_NAL_ANNEXB, hvcc, sizeof(hvcc), 90000,
      hevc_key, hevc_p, sizeof(hevc_key), sizeof(hevc_p), 3000 },
    { "mp4a", MOQ_CMAF_ENTRY_MP4A, MOQ_CMAF_SAMPLE_AAC_RAW, asc, sizeof(asc), 0,
      blob_a, blob_b, sizeof(blob_a), sizeof(blob_b), 1024 },
    { "av01", MOQ_CMAF_ENTRY_AV01, MOQ_CMAF_SAMPLE_AV1_OBU, av1c, sizeof(av1c), 90000,
      obu, obu, sizeof(obu), sizeof(obu), 3000 },
    { "Opus", MOQ_CMAF_ENTRY_OPUS, MOQ_CMAF_SAMPLE_OPUS_PACKET, dops, sizeof(dops), 0,
      opus, opus, sizeof(opus), sizeof(opus), 960 },
};

#define MAXB 4096

static bool encode_alone(const moq_locmaf_track_t *t, const uint8_t *x, size_t n,
                         uint8_t *canon, size_t *canon_n)
{
    lm_enc_t e;
    lm_dec_t d;
    uint8_t obj[MAXB];
    size_t on = 0;
    moq_locmaf_reason_t why;
    bool ok = false;
    if (lm_enc_open(&e, t, 0, 0, 0) != MOQ_OK) return false;
    if (lm_dec_open(&d, t, 0, 0, 0) != MOQ_OK) { lm_enc_close(&e); return false; }
    moq_locmaf_chunk_t ch;
    if (moq_locmaf_encode_chunk(&e.e, 0, 0, (moq_bytes_t){ x, n }, obj, MAXB, &on, &why) == MOQ_OK &&
        moq_locmaf_decoder_push(&d.d, 0, 0, (moq_bytes_t){ obj, on }, &ch, sizeof(ch), &why) == MOQ_OK &&
        moq_locmaf_write_cmaf(t, &ch, canon, MAXB, canon_n) == MOQ_OK)
        ok = true;
    lm_enc_close(&e);
    lm_dec_close(&d);
    return ok;
}

static void run_codec(const codec_t *c)
{
    moq_cmaf_packager_cfg_t cfg;
    moq_cmaf_packager_cfg_init(&cfg);
    cfg.sample_entry = c->entry;
    cfg.sample_format = c->format;
    cfg.codec_config = (moq_bytes_t){ c->config, c->config_len };
    cfg.timescale = c->timescale;
    cfg.width = 640;
    cfg.height = 480;
    cfg.samplerate = 48000;
    cfg.channel_count = 2;
    moq_cmaf_packager_t *p = NULL;
    moq_result_t rc = moq_cmaf_packager_create(NULL, &cfg, &p);
    if (rc != MOQ_OK) fprintf(stderr, "  codec %s\n", c->name);
    CHECK_RC(rc, MOQ_OK);
    if (rc != MOQ_OK) return;

    moq_cmaf_init_info_t init;
    CHECK_RC(moq_cmaf_parse_init_sized(moq_cmaf_packager_init_segment(p), &init, sizeof(init)),
             MOQ_OK);
    moq_locmaf_track_t t;
    rc = moq_locmaf_track_from_cmaf(&init, &t, sizeof(t));
    if (rc != MOQ_OK) fprintf(stderr, "  codec %s: header facts\n", c->name);
    CHECK_RC(rc, MOQ_OK);
    if (rc != MOQ_OK) { moq_cmaf_packager_destroy(p); return; }

    lm_enc_t e;
    lm_dec_t d;
    CHECK_RC(lm_enc_open(&e, &t, 0, 0, 0), MOQ_OK);
    CHECK_RC(lm_dec_open(&d, &t, 0, 0, 0), MOQ_OK);
    int64_t dts = 0;
    for (uint64_t o = 0; o < 6; o++) {
        /* one, two or three samples a chunk; the first of the group a key */
        size_t count = 1 + (size_t)(o % 3);
        moq_cmaf_packager_sample_t smp[3];
        for (size_t i = 0; i < count; i++) {
            bool key = o == 0 && i == 0;
            smp[i] = (moq_cmaf_packager_sample_t){
                .data = { key ? c->key : c->other, key ? c->key_len : c->other_len },
                .pts = dts, .dts = dts, .duration = c->duration, .keyframe = key };
            dts += c->duration;
        }
        moq_bytes_t frag = { NULL, 0 };
        rc = moq_cmaf_packager_write(p, smp, count, &frag);
        CHECK_RC(rc, MOQ_OK);
        if (rc != MOQ_OK) break;
        uint8_t src[MAXB], obj[MAXB], out[MAXB], canon[MAXB];
        CHECK(frag.len <= sizeof(src));
        memcpy(src, frag.data, frag.len);
        size_t sn = frag.len, on = 0, wn = 0, cn = 0;
        moq_locmaf_reason_t why;
        rc = moq_locmaf_encode_chunk(&e.e, 0, o, (moq_bytes_t){ src, sn }, obj, sizeof(obj),
                                     &on, &why);
        if (rc != MOQ_OK) fprintf(stderr, "  codec %s chunk %llu: %s\n", c->name,
                                  (unsigned long long)o, moq_locmaf_reason_name(why));
        CHECK_RC(rc, MOQ_OK);
        if (rc != MOQ_OK) break;
        moq_locmaf_encoder_commit(&e.e);
        CHECK(o == 0 ? obj[0] == MOQ_LOCMAF_ELEMENT_FULL_HEADER
                     : obj[0] == MOQ_LOCMAF_ELEMENT_DELTA_HEADER);
        moq_locmaf_chunk_t ch;
        memset(&ch, 0, sizeof(ch));
        rc = moq_locmaf_decoder_push(&d.d, 0, o, (moq_bytes_t){ obj, on }, &ch, sizeof(ch), &why);
        CHECK_RC(rc, MOQ_OK);
        if (rc != MOQ_OK || ch.sample_count > 4) break;
        CHECK_RC(moq_locmaf_write_cmaf(&t, &ch, out, sizeof(out), &wn), MOQ_OK);
        CHECK(encode_alone(&t, src, sn, canon, &cn));
        CHECK_BYTES(out, wn, canon, cn);
        /* the sample tables read from the source and the canonical chunk */
        moq_cmaf_sample_t a[4], b[4];
        moq_cmaf_fragment_info_t fa, fb;
        moq_cmaf_fragment_info_init(&fa, a, 4);
        moq_cmaf_fragment_info_init(&fb, b, 4);
        CHECK_RC(moq_cmaf_parse_fragment((moq_bytes_t){ src, sn }, &fa), MOQ_OK);
        CHECK_RC(moq_cmaf_parse_fragment((moq_bytes_t){ out, wn }, &fb), MOQ_OK);
        CHECK_EQ(fa.sample_count, fb.sample_count);
        CHECK_EQ(fa.base_decode_time, fb.base_decode_time);
        for (size_t i = 0; i < fa.sample_count && i < 4; i++) {
            uint32_t da = a[i].duration ? a[i].duration : t.trex_default_sample_duration;
            uint32_t db = b[i].duration ? b[i].duration : t.trex_default_sample_duration;
            uint32_t ga = a[i].flags ? a[i].flags : t.trex_default_sample_flags;
            uint32_t gb = b[i].flags ? b[i].flags : t.trex_default_sample_flags;
            CHECK(da == db && ga == gb);
            CHECK_EQ(a[i].size, b[i].size);
            CHECK_EQ(a[i].composition_offset, b[i].composition_offset);
            CHECK_EQ(ch.samples[i].size, b[i].size);
        }
        /* validation of the rebuilt chunk as an Object of the track */
        moq_cmaf_object_report_t rep;
        CHECK_RC(moq_cmaf_validate_object(&init, (moq_bytes_t){ out, wn }, &rep), MOQ_OK);
    }
    lm_enc_close(&e);
    lm_dec_close(&d);
    moq_cmaf_packager_destroy(p);
}

int main(void)
{
    for (size_t i = 0; i < sizeof(codecs) / sizeof(codecs[0]); i++) run_codec(&codecs[i]);
    return lm_finish("locmaf_packager_roundtrip");
}
