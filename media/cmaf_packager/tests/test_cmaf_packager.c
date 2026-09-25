#include <moq/cmaf_packager.h>

#include <moq/cmaf.h>

#include <stdio.h>
#include <string.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- Fixtures -------------------------------------------------------- */

/* AVCDecoderConfigurationRecord, lengthSizeMinusOne = 3 (4-byte prefixes). */
static const uint8_t kAvcC[] = {
    0x01, 0x64, 0x00, 0x1f, 0xff,
    0xe1, 0x00, 0x04, 0x67, 0x64, 0x00, 0x1f, /* 1 SPS */
    0x01, 0x00, 0x02, 0x68, 0xee,             /* 1 PPS */
};

/* Same record with lengthSizeMinusOne = 1 (2-byte prefixes). */
static const uint8_t kAvcC2Byte[] = {
    0x01, 0x64, 0x00, 0x1f, 0xfd,
    0xe1, 0x00, 0x04, 0x67, 0x64, 0x00, 0x1f,
    0x01, 0x00, 0x02, 0x68, 0xee,
};

/* HEVCDecoderConfigurationRecord: 23 bytes, lengthSizeMinusOne = 3 at [21]. */
static const uint8_t kHvcC[] = {
    0x01, 0x01, 0x60, 0x00, 0x00, 0x00, 0x90, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x5d, 0xf0, 0x00, 0xfc,
    0xfd, 0xf8, 0xf8, 0x00, 0x00, 0x0f, 0x00,
};

/* AV1CodecConfigurationRecord, four header bytes plus a config OBU. */
static const uint8_t kAv1C[] = {
    0x81, 0x00, 0x0c, 0x00, 0x0a, 0x0b, 0x00, 0x00,
};

/* AudioSpecificConfig: AAC-LC, 48 kHz, stereo. frameLengthFlag = 0 -> 1024. */
static const uint8_t kAsc[] = { 0x11, 0x90 };

/* Same, with frameLengthFlag set (bit 13, behind the 5-bit object type, the
 * 4-bit frequency index and the 4-bit channel config): 960 samples. */
static const uint8_t kAsc960[] = { 0x11, 0x94 };

/* OpusSpecificBox payload (dOps), mapping family 0, stereo. */
static const uint8_t kDops[] = {
    0x00, 0x02, 0x01, 0x38, 0x00, 0x00, 0xbb, 0x80, 0x00, 0x00, 0x00,
};

/* Two Annex B NALs: a 4-byte start code then a 3-byte one. */
static const uint8_t kAnnexB[] = {
    0x00, 0x00, 0x00, 0x01, 0x65, 0x11, 0x22, 0x33,
    0x00, 0x00, 0x01, 0x41, 0x44, 0x55,
};

static moq_bytes_t bytes(const uint8_t *d, size_t n)
{
    moq_bytes_t b = { d, n };
    return b;
}

#define BYTES(arr) bytes((arr), sizeof(arr))

/* -- Box lookup, so assertions can reach into what was written ------- */

static uint32_t rd32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16)
         | ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
}

static uint64_t rd64(const uint8_t *p)
{
    return ((uint64_t)rd32(p) << 32) | rd32(p + 4);
}

/* Depth-first search for a box type, returning its payload. Types are unique
 * enough in these fixtures that a recursive scan needs no path. */
static const uint8_t *find_box(const uint8_t *d, size_t len, const char *type,
                               size_t *out_len)
{
    size_t pos = 0;
    while (pos + 8 <= len) {
        uint32_t size = rd32(d + pos);
        if (size < 8 || pos + size > len) return NULL;
        if (memcmp(d + pos + 4, type, 4) == 0) {
            *out_len = size - 8;
            return d + pos + 8;
        }
        /* Recurse blindly: a leaf's payload simply yields no match. */
        size_t inner_len = 0;
        const uint8_t *inner = find_box(d + pos + 8, size - 8, type, &inner_len);
        if (inner) {
            *out_len = inner_len;
            return inner;
        }
        pos += size;
    }
    return NULL;
}

/* Every top-level box must be a moof followed by an mdat, nothing else. */
static bool is_moof_mdat_only(moq_bytes_t frag)
{
    size_t pos = 0;
    bool expect_moof = true;
    size_t chunks = 0;

    while (pos + 8 <= frag.len) {
        uint32_t size = rd32(frag.data + pos);
        const uint8_t *type = frag.data + pos + 4;
        if (size < 8 || pos + size > frag.len) return false;
        if (memcmp(type, expect_moof ? "moof" : "mdat", 4) != 0) return false;
        if (!expect_moof) chunks++;
        expect_moof = !expect_moof;
        pos += size;
    }

    return pos == frag.len && expect_moof && chunks == 1;
}

/* -- Config helpers -------------------------------------------------- */

static moq_cmaf_packager_cfg_t video_cfg(moq_cmaf_codec_kind_t kind,
                                         moq_bytes_t config)
{
    moq_cmaf_packager_cfg_t cfg;
    moq_cmaf_packager_cfg_init(&cfg);
    cfg.codec_kind = kind;
    cfg.codec_config = config;
    cfg.timescale = 90000;
    cfg.width = 1920;
    cfg.height = 1080;
    cfg.fps_num = 30;
    cfg.fps_den = 1;
    return cfg;
}

static moq_cmaf_packager_cfg_t audio_cfg(moq_cmaf_codec_kind_t kind,
                                         moq_bytes_t config)
{
    moq_cmaf_packager_cfg_t cfg;
    moq_cmaf_packager_cfg_init(&cfg);
    cfg.codec_kind = kind;
    cfg.codec_config = config;
    cfg.samplerate = 48000;
    cfg.channel_count = 2;
    return cfg;
}

static moq_cmaf_packager_sample_t sample_of(moq_bytes_t data, int64_t dts,
                                           int64_t pts, bool key)
{
    moq_cmaf_packager_sample_t s;
    memset(&s, 0, sizeof(s));
    s.data = data;
    s.dts = dts;
    s.pts = pts;
    s.keyframe = key;
    return s;
}

/* ================================================================== */

int main(void)
{
    /* -- 1. null / argument handling ---------------------------------- */
    {
        moq_cmaf_packager_cfg_init(NULL);
        moq_cmaf_packager_destroy(NULL);
        CHECK(moq_cmaf_packager_timescale(NULL) == 0);
        CHECK(moq_cmaf_packager_init_segment(NULL).data == NULL);

        moq_cmaf_packager_t *p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, NULL, &p) == MOQ_ERR_INVAL);

        moq_cmaf_packager_cfg_t cfg = video_cfg(MOQ_CMAF_CODEC_AVC, BYTES(kAvcC));
        CHECK(moq_cmaf_packager_create(NULL, &cfg, NULL) == MOQ_ERR_INVAL);

        /* An old caller whose struct predates a field is rejected, not
         * silently read past. */
        moq_cmaf_packager_cfg_t stale = cfg;
        stale.struct_size = 4;
        CHECK(moq_cmaf_packager_create(NULL, &stale, &p) == MOQ_ERR_INVAL);
    }

    /* -- 2. rejected configurations ----------------------------------- */
    {
        moq_cmaf_packager_t *p = NULL;

        moq_cmaf_packager_cfg_t unknown =
            video_cfg(MOQ_CMAF_CODEC_UNKNOWN, BYTES(kAvcC));
        CHECK(moq_cmaf_packager_create(NULL, &unknown, &p) == MOQ_ERR_INVAL);

        moq_cmaf_packager_cfg_t no_config =
            video_cfg(MOQ_CMAF_CODEC_AVC, bytes(NULL, 0));
        CHECK(moq_cmaf_packager_create(NULL, &no_config, &p) == MOQ_ERR_INVAL);

        moq_cmaf_packager_cfg_t no_size = video_cfg(MOQ_CMAF_CODEC_AVC, BYTES(kAvcC));
        no_size.width = 0;
        CHECK(moq_cmaf_packager_create(NULL, &no_size, &p) == MOQ_ERR_INVAL);

        /* Video with no timescale and no way to derive one. */
        moq_cmaf_packager_cfg_t no_ts = video_cfg(MOQ_CMAF_CODEC_AVC, BYTES(kAvcC));
        no_ts.timescale = 0;
        CHECK(moq_cmaf_packager_create(NULL, &no_ts, &p) == MOQ_ERR_INVAL);

        /* Video with a timescale but no fps and no explicit duration. */
        moq_cmaf_packager_cfg_t no_dur = video_cfg(MOQ_CMAF_CODEC_AVC, BYTES(kAvcC));
        no_dur.fps_num = 0;
        no_dur.fps_den = 0;
        CHECK(moq_cmaf_packager_create(NULL, &no_dur, &p) == MOQ_ERR_INVAL);

        /* ... which an explicit duration fixes. */
        no_dur.default_sample_duration = 3000;
        CHECK(moq_cmaf_packager_create(NULL, &no_dur, &p) == MOQ_OK);
        moq_cmaf_packager_destroy(p);
        p = NULL;

        /* Annex B is meaningless for a codec with no NALs. */
        moq_cmaf_packager_cfg_t bad_fmt = audio_cfg(MOQ_CMAF_CODEC_AAC, BYTES(kAsc));
        bad_fmt.sample_format = MOQ_CMAF_SAMPLE_ANNEXB;
        CHECK(moq_cmaf_packager_create(NULL, &bad_fmt, &p) == MOQ_ERR_INVAL);

        /* Half a source timebase says nothing. */
        moq_cmaf_packager_cfg_t half_tb = video_cfg(MOQ_CMAF_CODEC_AVC, BYTES(kAvcC));
        half_tb.source_timebase_num = 1;
        CHECK(moq_cmaf_packager_create(NULL, &half_tb, &p) == MOQ_ERR_INVAL);

        /* timescale * source_timebase_num has to stay rescalable. */
        moq_cmaf_packager_cfg_t huge_tb = video_cfg(MOQ_CMAF_CODEC_AVC, BYTES(kAvcC));
        huge_tb.source_timebase_num = 1000000000;
        huge_tb.source_timebase_den = 1;
        CHECK(moq_cmaf_packager_create(NULL, &huge_tb, &p) == MOQ_ERR_INVAL);

        /* A truncated avcC cannot yield a NAL length size. */
        moq_cmaf_packager_cfg_t short_rec =
            video_cfg(MOQ_CMAF_CODEC_AVC, bytes(kAvcC, 4));
        CHECK(moq_cmaf_packager_create(NULL, &short_rec, &p) == MOQ_ERR_PROTO);

        moq_cmaf_packager_cfg_t short_hvcc =
            video_cfg(MOQ_CMAF_CODEC_HEVC, bytes(kHvcC, 20));
        CHECK(moq_cmaf_packager_create(NULL, &short_hvcc, &p) == MOQ_ERR_PROTO);
    }

    /* -- 3. AVC round-trip through the parser ------------------------- */
    {
        moq_cmaf_packager_cfg_t cfg = video_cfg(MOQ_CMAF_CODEC_AVC, BYTES(kAvcC));
        cfg.rebase_timestamps = true;

        moq_cmaf_packager_t *p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_OK);
        if (!p) goto done;

        CHECK(moq_cmaf_packager_timescale(p) == 90000);

        moq_bytes_t init = moq_cmaf_packager_init_segment(p);
        CHECK(init.len > 0);

        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init(init, &info) == MOQ_OK);
        CHECK(info.codec_kind == MOQ_CMAF_CODEC_AVC);
        CHECK(info.timescale == 90000);
        CHECK(info.width == 1920);
        CHECK(info.height == 1080);
        CHECK(info.track_id == 1);
        CHECK(!info.has_cenc);
        /* The record comes back exactly as it went in. */
        CHECK(info.codec_config.len == sizeof(kAvcC));
        CHECK(info.codec_config.data &&
              memcmp(info.codec_config.data, kAvcC, sizeof(kAvcC)) == 0);

        /* First chunk: dts 3000 with rebasing, so tfdt must be 0. */
        moq_cmaf_packager_sample_t s = sample_of(BYTES(kAnnexB), 3000, 6000, true);
        moq_bytes_t frag;
        CHECK(moq_cmaf_packager_write(p, &s, 1, &frag) == MOQ_OK);
        CHECK(frag.len > 0);
        CHECK(is_moof_mdat_only(frag));

        moq_cmaf_object_report_t rep;
        moq_cmaf_object_report_init(&rep);
        CHECK(moq_cmaf_validate_object(&info, frag, &rep) == MOQ_OK);
        CHECK(rep.valid);
        CHECK(rep.reason == MOQ_CMAF_OK);
        CHECK(rep.chunk_count == 1);
        CHECK(rep.track_id == 1);
        CHECK(rep.starts_with_sync);

        moq_cmaf_sample_t table[8];
        moq_cmaf_fragment_info_t frag_info;
        moq_cmaf_fragment_info_init(&frag_info, table, 8);
        CHECK(moq_cmaf_parse_fragment(frag, &frag_info) == MOQ_OK);
        CHECK(frag_info.sample_count == 1);
        CHECK(frag_info.has_base_decode_time);
        CHECK(frag_info.base_decode_time == 0);
        CHECK(frag_info.track_id == 1);
        CHECK(table[0].duration == 3000);           /* 90000 / 30 fps */
        CHECK(table[0].composition_offset == 3000); /* pts - dts */
        CHECK(moq_cmaf_sap_from_sample_flags(table[0].flags) != MOQ_SAP_NONE);

        /* Annex B became length prefixes: NALs of 4 and 3 bytes, each with a
         * 4-byte prefix, so 15 bytes reach the mdat. */
        CHECK(table[0].size == 15);
        CHECK(frag_info.mdat.len == 15);
        if (frag_info.mdat.len == 15) {
            CHECK(rd32(frag_info.mdat.data) == 4);
            CHECK(frag_info.mdat.data[4] == 0x65);
            CHECK(rd32(frag_info.mdat.data + 8) == 3);
            CHECK(frag_info.mdat.data[12] == 0x41);
        }

        /* Second chunk: a non-sync sample, one frame later. */
        s = sample_of(BYTES(kAnnexB), 6000, 6000, false);
        CHECK(moq_cmaf_packager_write(p, &s, 1, &frag) == MOQ_OK);
        moq_cmaf_fragment_info_init(&frag_info, table, 8);
        CHECK(moq_cmaf_parse_fragment(frag, &frag_info) == MOQ_OK);
        CHECK(frag_info.base_decode_time == 3000);
        CHECK(table[0].composition_offset == 0);
        CHECK(moq_cmaf_sap_from_sample_flags(table[0].flags) == MOQ_SAP_NONE);

        /* Sequence numbers advance across chunks. */
        size_t mfhd_len = 0;
        const uint8_t *mfhd = find_box(frag.data, frag.len, "mfhd", &mfhd_len);
        CHECK(mfhd && mfhd_len >= 8);
        if (mfhd && mfhd_len >= 8) CHECK(rd32(mfhd + 4) == 2);

        /* tfhd states default-base-is-moof, the sample description index and
         * all three sample defaults: ver/flags + track_ID + sdi + duration +
         * size + flags = 24 bytes of payload. */
        size_t tfhd_len = 0;
        const uint8_t *tfhd = find_box(frag.data, frag.len, "tfhd", &tfhd_len);
        CHECK(tfhd && tfhd_len == 24);
        if (tfhd && tfhd_len == 24) {
            CHECK((rd32(tfhd) & 0x00ffffffu) == 0x02003a);
            CHECK(rd32(tfhd + 4) == 1);          /* track_ID */
            CHECK(rd32(tfhd + 8) == 1);          /* sample_description_index */
            CHECK(rd32(tfhd + 12) == 3000);      /* default_sample_duration */
            CHECK(rd32(tfhd + 16) == 15);        /* default_sample_size */
            CHECK(rd32(tfhd + 20) == 0x01010000);/* default flags: non-sync */
        }

        /* No base_data_offset, so the trun offset is exactly the moof size
         * plus the mdat header. */
        const uint32_t moof_size = rd32(frag.data);
        size_t trun_len = 0;
        const uint8_t *trun = find_box(frag.data, frag.len, "trun", &trun_len);
        CHECK(trun && trun_len >= 12);
        if (trun && trun_len >= 12) CHECK(rd32(trun + 8) == moof_size + 8);

        /* One sample matching every default, non-sync like the default: the
         * trun restates nothing, not even first-sample-flags. */
        if (trun) CHECK((rd32(trun) & 0x00ffffffu) == 0x000001);

        moq_cmaf_packager_destroy(p);
    }

    /* -- 4. HEVC and AV1 init segments -------------------------------- */
    {
        moq_cmaf_packager_cfg_t cfg = video_cfg(MOQ_CMAF_CODEC_HEVC, BYTES(kHvcC));
        moq_cmaf_packager_t *p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_OK);
        if (p) {
            moq_cmaf_init_info_t info;
            moq_cmaf_init_info_init(&info);
            CHECK(moq_cmaf_parse_init(moq_cmaf_packager_init_segment(p), &info)
                  == MOQ_OK);
            CHECK(info.codec_kind == MOQ_CMAF_CODEC_HEVC);
            CHECK(info.codec_config.len == sizeof(kHvcC));
            CHECK(memcmp(info.codec_config.data, kHvcC, sizeof(kHvcC)) == 0);
            moq_cmaf_packager_destroy(p);
        }

        cfg = video_cfg(MOQ_CMAF_CODEC_AV1, BYTES(kAv1C));
        p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_OK);
        if (p) {
            moq_cmaf_init_info_t info;
            moq_cmaf_init_info_init(&info);
            CHECK(moq_cmaf_parse_init(moq_cmaf_packager_init_segment(p), &info)
                  == MOQ_OK);
            CHECK(info.codec_kind == MOQ_CMAF_CODEC_AV1);
            CHECK(info.codec_config.len == sizeof(kAv1C));
            CHECK(memcmp(info.codec_config.data, kAv1C, sizeof(kAv1C)) == 0);

            /* AV1 defaults to elementary: OBUs reach the mdat untouched. */
            static const uint8_t obu[] = { 0x12, 0x00, 0xaa, 0xbb };
            moq_cmaf_packager_sample_t s = sample_of(BYTES(obu), 0, 0, true);
            moq_bytes_t frag;
            CHECK(moq_cmaf_packager_write(p, &s, 1, &frag) == MOQ_OK);

            moq_cmaf_sample_t table[4];
            moq_cmaf_fragment_info_t fi;
            moq_cmaf_fragment_info_init(&fi, table, 4);
            CHECK(moq_cmaf_parse_fragment(frag, &fi) == MOQ_OK);
            CHECK(fi.mdat.len == sizeof(obu));
            CHECK(memcmp(fi.mdat.data, obu, sizeof(obu)) == 0);
            moq_cmaf_packager_destroy(p);
        }
    }

    /* -- 5. AAC: esds round-trip, ADTS stripping, frame length -------- */
    {
        moq_cmaf_packager_cfg_t cfg = audio_cfg(MOQ_CMAF_CODEC_AAC, BYTES(kAsc));
        cfg.avg_bitrate = 128000;

        moq_cmaf_packager_t *p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_OK);
        if (!p) goto done;

        /* timescale 0 for audio resolves to the samplerate. */
        CHECK(moq_cmaf_packager_timescale(p) == 48000);

        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init(moq_cmaf_packager_init_segment(p), &info)
              == MOQ_OK);
        CHECK(info.codec_kind == MOQ_CMAF_CODEC_AAC);
        CHECK(info.timescale == 48000);
        CHECK(info.samplerate == 48000);
        CHECK(info.channel_count == 2);
        /* The ASC survives the esds descriptor chain intact. */
        CHECK(info.codec_config.len == sizeof(kAsc));
        CHECK(info.codec_config.data &&
              memcmp(info.codec_config.data, kAsc, sizeof(kAsc)) == 0);

        /* An ADTS header must not reach the mdat. */
        static const uint8_t adts_frame[] = {
            0xff, 0xf1, 0x4c, 0x80, 0x01, 0x1f, 0xfc, /* 7-byte ADTS */
            0x21, 0x1a, 0x2b, 0x3c,
        };
        moq_cmaf_packager_sample_t s = sample_of(BYTES(adts_frame), 0, 0, true);
        moq_bytes_t frag;
        CHECK(moq_cmaf_packager_write(p, &s, 1, &frag) == MOQ_OK);

        moq_cmaf_sample_t table[4];
        moq_cmaf_fragment_info_t fi;
        moq_cmaf_fragment_info_init(&fi, table, 4);
        CHECK(moq_cmaf_parse_fragment(frag, &fi) == MOQ_OK);
        CHECK(fi.mdat.len == 4);
        CHECK(fi.mdat.len == 4 && memcmp(fi.mdat.data, adts_frame + 7, 4) == 0);
        CHECK(table[0].duration == 1024);   /* frameLengthFlag clear */

        /* An ADTS header with no frame behind it is rejected. */
        moq_cmaf_packager_sample_t empty =
            sample_of(bytes(adts_frame, 7), 1024, 1024, true);
        CHECK(moq_cmaf_packager_write(p, &empty, 1, &frag) == MOQ_ERR_PROTO);

        moq_cmaf_packager_destroy(p);

        /* frameLengthFlag set -> 960 samples per frame. */
        cfg = audio_cfg(MOQ_CMAF_CODEC_AAC, BYTES(kAsc960));
        p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_OK);
        if (p) {
            static const uint8_t raw[] = { 0x21, 0x1a };
            moq_cmaf_packager_sample_t rs = sample_of(BYTES(raw), 0, 0, true);
            moq_bytes_t f;
            CHECK(moq_cmaf_packager_write(p, &rs, 1, &f) == MOQ_OK);
            moq_cmaf_fragment_info_init(&fi, table, 4);
            CHECK(moq_cmaf_parse_fragment(f, &fi) == MOQ_OK);
            CHECK(table[0].duration == 960);
            moq_cmaf_packager_destroy(p);
        }
    }

    /* -- 6. Opus ------------------------------------------------------ */
    {
        moq_cmaf_packager_cfg_t cfg = audio_cfg(MOQ_CMAF_CODEC_OPUS, BYTES(kDops));
        moq_cmaf_packager_t *p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_OK);
        if (!p) goto done;

        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init(moq_cmaf_packager_init_segment(p), &info)
              == MOQ_OK);
        CHECK(info.codec_kind == MOQ_CMAF_CODEC_OPUS);
        CHECK(info.samplerate == 48000);
        CHECK(info.channel_count == 2);
        CHECK(info.codec_config.len == sizeof(kDops));
        CHECK(memcmp(info.codec_config.data, kDops, sizeof(kDops)) == 0);

        moq_cmaf_sample_t table[4];
        moq_cmaf_fragment_info_t fi;
        moq_bytes_t frag;

        /* The frame length comes from each packet's TOC byte (RFC 6716 3.1),
         * never from an assumed 20 ms: config = toc >> 3, frames = toc & 3. */
        static const struct {
            uint8_t  toc;
            uint8_t  second;   /* frame count byte, for code 3 */
            uint32_t samples;
            const char *what;
        } kToc[] = {
            { 0x00, 0x00,  480, "SILK NB 10 ms"  },  /* config 0,  1 frame  */
            { 0x08, 0x00,  960, "SILK NB 20 ms"  },  /* config 1,  1 frame  */
            { 0x18, 0x00, 2880, "SILK NB 60 ms"  },  /* config 3,  1 frame  */
            { 0x60, 0x00,  480, "hybrid 10 ms"   },  /* config 12, 1 frame  */
            { 0x80, 0x00,  120, "CELT 2.5 ms"    },  /* config 16, 1 frame  */
            { 0x89, 0x00,  480, "CELT 5 ms x2"   },  /* config 17, 2 frames */
            { 0x0b, 0x03, 2880, "SILK 20 ms x3"  },  /* config 1,  3 frames */
        };

        for (size_t i = 0; i < sizeof(kToc) / sizeof(kToc[0]); i++) {
            const uint8_t pkt[] = { kToc[i].toc, kToc[i].second, 0x11, 0x22 };
            moq_cmaf_packager_sample_t s =
                sample_of(bytes(pkt, sizeof(pkt)), (int64_t)i * 5760,
                          (int64_t)i * 5760, true);
            if (moq_cmaf_packager_write(p, &s, 1, &frag) != MOQ_OK) {
                fprintf(stderr, "FAIL: opus write %s\n", kToc[i].what);
                failures++;
                continue;
            }
            moq_cmaf_fragment_info_init(&fi, table, 4);
            CHECK(moq_cmaf_parse_fragment(frag, &fi) == MOQ_OK);
            if (table[0].duration != kToc[i].samples) {
                fprintf(stderr, "FAIL: opus %s: duration %u, want %u\n",
                        kToc[i].what, table[0].duration, kToc[i].samples);
                failures++;
            }
        }

        /* An explicit per-sample duration still overrides the TOC. */
        static const uint8_t packet[] = { 0x08, 0x01, 0x02, 0x03 };
        moq_cmaf_packager_sample_t s = sample_of(BYTES(packet), 0, 0, true);
        s.duration = 1234;
        CHECK(moq_cmaf_packager_write(p, &s, 1, &frag) == MOQ_OK);
        moq_cmaf_fragment_info_init(&fi, table, 4);
        CHECK(moq_cmaf_parse_fragment(frag, &fi) == MOQ_OK);
        CHECK(table[0].duration == 1234);
        CHECK(fi.mdat.len == sizeof(packet));

        /* A frame-count byte of zero cannot yield a duration. */
        static const uint8_t bad[] = { 0x0b, 0x00, 0x02 };
        moq_cmaf_packager_sample_t z = sample_of(BYTES(bad), 0, 0, true);
        CHECK(moq_cmaf_packager_write(p, &z, 1, &frag) == MOQ_ERR_PROTO);

        moq_cmaf_packager_destroy(p);
    }

    /* -- 6b. trun only restates what differs from the tfhd defaults ---- */
    {
        moq_cmaf_packager_cfg_t cfg = video_cfg(MOQ_CMAF_CODEC_AVC, BYTES(kAvcC));
        moq_cmaf_packager_t *p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_OK);
        if (!p) goto done;

        moq_bytes_t frag;

        /* A sync sample opening a run of non-sync ones: the default comes from
         * sample 1, so only the first needs an override. */
        moq_cmaf_packager_sample_t s[4];
        for (int i = 0; i < 4; i++)
            s[i] = sample_of(BYTES(kAnnexB), i * 3000, i * 3000, i == 0);
        CHECK(moq_cmaf_packager_write(p, s, 4, &frag) == MOQ_OK);

        size_t trun_len = 0;
        const uint8_t *trun = find_box(frag.data, frag.len, "trun", &trun_len);
        CHECK(trun != NULL);
        if (trun) {
            /* data-offset + first-sample-flags, nothing per sample: identical
             * payloads mean identical sizes and durations. */
            CHECK((rd32(trun) & 0x00ffffffu) == 0x000005);
            CHECK(rd32(trun + 12) == 0x02000000);   /* the sync override */
        }

        size_t tfhd_len = 0;
        const uint8_t *tfhd = find_box(frag.data, frag.len, "tfhd", &tfhd_len);
        if (tfhd && tfhd_len == 24)
            CHECK(rd32(tfhd + 20) == 0x01010000);   /* default from sample 1 */

        /* Parsing resolves defaults and overrides back to per-sample values. */
        moq_cmaf_sample_t table[4];
        moq_cmaf_fragment_info_t fi;
        moq_cmaf_fragment_info_init(&fi, table, 4);
        CHECK(moq_cmaf_parse_fragment(frag, &fi) == MOQ_OK);
        CHECK(fi.sample_count == 4);
        CHECK(moq_cmaf_sap_from_sample_flags(table[0].flags) != MOQ_SAP_NONE);
        for (int i = 1; i < 4; i++)
            CHECK(moq_cmaf_sap_from_sample_flags(table[i].flags) == MOQ_SAP_NONE);
        for (int i = 0; i < 4; i++) {
            CHECK(table[i].duration == 3000);
            CHECK(table[i].size == 15);
        }

        /* Varying durations force the per-sample field back on. */
        s[2].duration = 1500;
        CHECK(moq_cmaf_packager_write(p, s, 4, &frag) == MOQ_OK);
        trun = find_box(frag.data, frag.len, "trun", &trun_len);
        CHECK(trun && (rd32(trun) & 0x000100u) != 0);
        moq_cmaf_fragment_info_init(&fi, table, 4);
        CHECK(moq_cmaf_parse_fragment(frag, &fi) == MOQ_OK);
        CHECK(table[2].duration == 1500);
        CHECK(table[3].duration == 3000);

        moq_cmaf_packager_destroy(p);
    }

    /* -- 7. Multiple samples in one chunk ----------------------------- */
    {
        moq_cmaf_packager_cfg_t cfg = video_cfg(MOQ_CMAF_CODEC_AVC, BYTES(kAvcC));
        moq_cmaf_packager_t *p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_OK);
        if (!p) goto done;

        /* More than the on-stack size table holds, to exercise the heap path. */
        enum { N = 20 };
        moq_cmaf_packager_sample_t s[N];
        for (int i = 0; i < N; i++)
            s[i] = sample_of(BYTES(kAnnexB), i * 3000, i * 3000, i == 0);

        moq_bytes_t frag;
        CHECK(moq_cmaf_packager_write(p, s, N, &frag) == MOQ_OK);
        CHECK(is_moof_mdat_only(frag));

        moq_cmaf_sample_t table[N];
        moq_cmaf_fragment_info_t fi;
        moq_cmaf_fragment_info_init(&fi, table, N);
        CHECK(moq_cmaf_parse_fragment(frag, &fi) == MOQ_OK);
        CHECK(fi.sample_count == N);
        CHECK(fi.base_decode_time == 0);
        CHECK(fi.mdat.len == (size_t)N * 15);
        CHECK(moq_cmaf_sap_from_sample_flags(table[0].flags) != MOQ_SAP_NONE);
        CHECK(moq_cmaf_sap_from_sample_flags(table[1].flags) == MOQ_SAP_NONE);

        moq_cmaf_object_report_t rep;
        moq_cmaf_object_report_init(&rep);
        CHECK(moq_cmaf_validate_object(NULL, frag, &rep) == MOQ_OK);
        CHECK(rep.valid);

        moq_cmaf_packager_destroy(p);
    }

    /* -- 8. Sample format handling ------------------------------------ */
    {
        /* A 2-byte length size from the avcC is what gets written. */
        moq_cmaf_packager_cfg_t cfg =
            video_cfg(MOQ_CMAF_CODEC_AVC, BYTES(kAvcC2Byte));
        moq_cmaf_packager_t *p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_OK);
        if (!p) goto done;

        moq_cmaf_packager_sample_t s = sample_of(BYTES(kAnnexB), 0, 0, true);
        moq_bytes_t frag;
        CHECK(moq_cmaf_packager_write(p, &s, 1, &frag) == MOQ_OK);

        moq_cmaf_sample_t table[4];
        moq_cmaf_fragment_info_t fi;
        moq_cmaf_fragment_info_init(&fi, table, 4);
        CHECK(moq_cmaf_parse_fragment(frag, &fi) == MOQ_OK);
        /* 2 NALs of 4 and 3 bytes, 2-byte prefixes: 11 bytes. */
        CHECK(fi.mdat.len == 11);
        if (fi.mdat.len == 11) {
            CHECK(fi.mdat.data[0] == 0 && fi.mdat.data[1] == 4);
            CHECK(fi.mdat.data[6] == 0 && fi.mdat.data[7] == 3);
        }

        /* A sample with no start code is not Annex B. */
        static const uint8_t no_sc[] = { 0x65, 0x11, 0x22, 0x33 };
        moq_cmaf_packager_sample_t bad = sample_of(BYTES(no_sc), 0, 0, true);
        CHECK(moq_cmaf_packager_write(p, &bad, 1, &frag) == MOQ_ERR_PROTO);

        /* An empty sample is a caller error, not a malformed stream. */
        moq_cmaf_packager_sample_t none = sample_of(bytes(NULL, 0), 0, 0, true);
        CHECK(moq_cmaf_packager_write(p, &none, 1, &frag) == MOQ_ERR_INVAL);
        CHECK(moq_cmaf_packager_write(p, &s, 0, &frag) == MOQ_ERR_INVAL);

        moq_cmaf_packager_destroy(p);

        /* LENGTH_PREFIXED passes bytes through but validates the prefixes. */
        cfg = video_cfg(MOQ_CMAF_CODEC_AVC, BYTES(kAvcC));
        cfg.sample_format = MOQ_CMAF_SAMPLE_LENGTH_PREFIXED;
        p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_OK);
        if (p) {
            static const uint8_t prefixed[] = {
                0x00, 0x00, 0x00, 0x03, 0x65, 0x11, 0x22,
            };
            moq_cmaf_packager_sample_t ok = sample_of(BYTES(prefixed), 0, 0, true);
            CHECK(moq_cmaf_packager_write(p, &ok, 1, &frag) == MOQ_OK);
            moq_cmaf_fragment_info_init(&fi, table, 4);
            CHECK(moq_cmaf_parse_fragment(frag, &fi) == MOQ_OK);
            CHECK(fi.mdat.len == sizeof(prefixed));
            CHECK(memcmp(fi.mdat.data, prefixed, sizeof(prefixed)) == 0);

            /* A length running past the end is rejected. */
            static const uint8_t overrun[] = {
                0x00, 0x00, 0x00, 0x40, 0x65, 0x11,
            };
            moq_cmaf_packager_sample_t bad_len =
                sample_of(BYTES(overrun), 0, 0, true);
            CHECK(moq_cmaf_packager_write(p, &bad_len, 1, &frag) == MOQ_ERR_PROTO);

            moq_cmaf_packager_destroy(p);
        }
    }

    /* -- 9. Timebase rescaling and rebasing --------------------------- */
    {
        /* Timestamps in microseconds, track at 90 kHz. */
        moq_cmaf_packager_cfg_t cfg = video_cfg(MOQ_CMAF_CODEC_AVC, BYTES(kAvcC));
        cfg.source_timebase_num = 1;
        cfg.source_timebase_den = 1000000;
        cfg.rebase_timestamps = true;

        moq_cmaf_packager_t *p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_OK);
        if (!p) goto done;

        moq_cmaf_sample_t table[4];
        moq_cmaf_fragment_info_t fi;
        moq_bytes_t frag;

        /* Origin at 5 s; the first tfdt is 0 whatever the offset. */
        moq_cmaf_packager_sample_t s =
            sample_of(BYTES(kAnnexB), 5000000, 5000000, true);
        CHECK(moq_cmaf_packager_write(p, &s, 1, &frag) == MOQ_OK);
        moq_cmaf_fragment_info_init(&fi, table, 4);
        CHECK(moq_cmaf_parse_fragment(frag, &fi) == MOQ_OK);
        CHECK(fi.base_decode_time == 0);

        /* 1/30 s later: 33333 us -> 2999 ticks at 90 kHz (truncated). */
        s = sample_of(BYTES(kAnnexB), 5033333, 5033333, false);
        CHECK(moq_cmaf_packager_write(p, &s, 1, &frag) == MOQ_OK);
        moq_cmaf_fragment_info_init(&fi, table, 4);
        CHECK(moq_cmaf_parse_fragment(frag, &fi) == MOQ_OK);
        CHECK(fi.base_decode_time == 2999);

        moq_cmaf_packager_destroy(p);

        /* Without rebasing the caller's timeline is preserved. */
        cfg.rebase_timestamps = false;
        p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_OK);
        if (p) {
            s = sample_of(BYTES(kAnnexB), 2000000, 2000000, true);
            CHECK(moq_cmaf_packager_write(p, &s, 1, &frag) == MOQ_OK);
            moq_cmaf_fragment_info_init(&fi, table, 4);
            CHECK(moq_cmaf_parse_fragment(frag, &fi) == MOQ_OK);
            CHECK(fi.base_decode_time == 180000);   /* 2 s at 90 kHz */

            /* A negative decode time has no tfdt representation. */
            s = sample_of(BYTES(kAnnexB), -1000, -1000, true);
            CHECK(moq_cmaf_packager_write(p, &s, 1, &frag) == MOQ_ERR_INVAL);

            moq_cmaf_packager_destroy(p);
        }

        /* A rejected chunk must not define the rebasing origin. */
        cfg = video_cfg(MOQ_CMAF_CODEC_AVC, BYTES(kAvcC));
        cfg.rebase_timestamps = true;
        p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_OK);
        if (p) {
            static const uint8_t no_sc[] = { 0x65, 0x11, 0x22 };
            moq_cmaf_packager_sample_t bad = sample_of(BYTES(no_sc), 3000, 3000, true);
            CHECK(moq_cmaf_packager_write(p, &bad, 1, &frag) == MOQ_ERR_PROTO);

            s = sample_of(BYTES(kAnnexB), 9000, 9000, true);
            CHECK(moq_cmaf_packager_write(p, &s, 1, &frag) == MOQ_OK);
            moq_cmaf_fragment_info_init(&fi, table, 4);
            CHECK(moq_cmaf_parse_fragment(frag, &fi) == MOQ_OK);
            CHECK(fi.base_decode_time == 0);

            moq_cmaf_packager_destroy(p);
        }
    }

    /* -- 10. track_id propagation ------------------------------------- */
    {
        moq_cmaf_packager_cfg_t cfg = video_cfg(MOQ_CMAF_CODEC_AVC, BYTES(kAvcC));
        cfg.track_id = 7;

        moq_cmaf_packager_t *p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_OK);
        if (!p) goto done;

        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init(moq_cmaf_packager_init_segment(p), &info)
              == MOQ_OK);
        CHECK(info.track_id == 7);

        moq_cmaf_packager_sample_t s = sample_of(BYTES(kAnnexB), 0, 0, true);
        moq_bytes_t frag;
        CHECK(moq_cmaf_packager_write(p, &s, 1, &frag) == MOQ_OK);

        /* The init and the chunk agree, which is what the validator checks. */
        moq_cmaf_object_report_t rep;
        moq_cmaf_object_report_init(&rep);
        CHECK(moq_cmaf_validate_object(&info, frag, &rep) == MOQ_OK);
        CHECK(rep.valid);
        CHECK(rep.track_id == 7);

        /* A mismatched init is caught. */
        moq_cmaf_init_info_t other = info;
        other.track_id = 3;
        moq_cmaf_object_report_init(&rep);
        CHECK(moq_cmaf_validate_object(&other, frag, &rep) == MOQ_ERR_PROTO);
        CHECK(rep.reason == MOQ_CMAF_ERR_TRACK_ID_MISMATCH);

        moq_cmaf_packager_destroy(p);
    }

    /* -- 11. The fragment buffer is reused, not reallocated per call --- */
    {
        moq_cmaf_packager_cfg_t cfg = video_cfg(MOQ_CMAF_CODEC_AVC, BYTES(kAvcC));
        moq_cmaf_packager_t *p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_OK);
        if (!p) goto done;

        moq_bytes_t a, b;
        moq_cmaf_packager_sample_t s = sample_of(BYTES(kAnnexB), 0, 0, true);
        CHECK(moq_cmaf_packager_write(p, &s, 1, &a) == MOQ_OK);
        const size_t first_len = a.len;

        s = sample_of(BYTES(kAnnexB), 3000, 3000, false);
        CHECK(moq_cmaf_packager_write(p, &s, 1, &b) == MOQ_OK);
        /* Same shape, same size: the second call overwrote the first. */
        CHECK(b.len == first_len);

        moq_cmaf_packager_destroy(p);
    }

done:
    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
