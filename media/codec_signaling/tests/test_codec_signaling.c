#include <moq/codec_signaling.h>
#include <stdio.h>
#include <string.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* Compare produced output against an expected NUL-terminated string. The
 * formatter output is NOT NUL-terminated, so compare by length + bytes. */
#define CHECK_STR(buf, len, expected) do { \
    const char *_e = (expected); \
    size_t _el = strlen(_e); \
    if ((len) != _el || memcmp((buf), _e, _el) != 0) { \
        fprintf(stderr, "FAIL: %s:%d: got \"%.*s\" (len %zu), expected \"%s\"\n", \
                __FILE__, __LINE__, (int)(len), (const char *)(buf), \
                (size_t)(len), _e); \
        failures++; \
    } \
} while (0)

static moq_bytes_t bytes(const uint8_t *p, size_t n)
{
    return (moq_bytes_t){ p, n };
}

static moq_bytes_t entry(const char *s)
{
    return (moq_bytes_t){ (const uint8_t *)s, strlen(s) };
}

int main(void)
{
    /* -- cfg_init stamps struct_size -------------------------------- */
    {
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        CHECK(cfg.struct_size == sizeof(moq_codec_string_cfg_t));
        CHECK(cfg.config_format == 0);
        CHECK(cfg.has_mp4_object_type_indication == false);
        /* NULL-safe */
        moq_codec_string_cfg_init(NULL);
    }

    /* -- AVC avcC -> avc1.64001e ------------------------------------ */
    {
        const uint8_t avcc[] = { 0x01, 0x64, 0x00, 0x1e, 0xff };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AVCC;
        cfg.sample_entry = entry("avc1");
        cfg.decoder_config = bytes(avcc, sizeof(avcc));

        uint8_t out[32];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_string_format(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK_STR(out, out_len, "avc1.64001e");
    }

    /* -- avc3 distinction preserved --------------------------------- */
    {
        const uint8_t avcc[] = { 0x01, 0x64, 0x00, 0x1e };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AVCC;
        cfg.sample_entry = entry("avc3");
        cfg.decoder_config = bytes(avcc, sizeof(avcc));

        uint8_t out[32];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_string_format(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK_STR(out, out_len, "avc3.64001e");
    }

    /* -- AAC-LC ASC -> mp4a.40.2 ------------------------------------ */
    {
        const uint8_t asc[] = { 0x12, 0x10 }; /* audioObjectType = 2 */
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AAC_ASC;
        cfg.sample_entry = entry("mp4a");
        cfg.has_mp4_object_type_indication = true;
        cfg.mp4_object_type_indication = 0x40;
        cfg.decoder_config = bytes(asc, sizeof(asc));

        uint8_t out[32];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_string_format(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK_STR(out, out_len, "mp4a.40.2");
    }

    /* -- AAC escape (audioObjectType 31 -> extension) --------------- */
    {
        /* byte0: 5-bit AOT = 31 (0b11111 << 3) => 0xf8.
         * Extension 6 bits spanning byte1[2:0]<<3 | byte2[7:5].
         * Choose byte1=0x08, byte2=0x00 => ((0x08 & 7)=0)<<3 | (0>>5)=0 => 0,
         * object_type = 32 + 0 = 32. */
        const uint8_t asc[] = { 0xf8, 0x08, 0x00 };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AAC_ASC;
        cfg.sample_entry = entry("mp4a");
        cfg.has_mp4_object_type_indication = true;
        cfg.mp4_object_type_indication = 0x40;
        cfg.decoder_config = bytes(asc, sizeof(asc));

        uint8_t out[32];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_string_format(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK_STR(out, out_len, "mp4a.40.32");
    }

    /* -- Short buffer -> MOQ_ERR_BUFFER, out_len set, buf untouched -- */
    {
        const uint8_t avcc[] = { 0x01, 0x64, 0x00, 0x1e };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AVCC;
        cfg.sample_entry = entry("avc1");
        cfg.decoder_config = bytes(avcc, sizeof(avcc));

        uint8_t out[4];
        memset(out, 0xcc, sizeof(out));
        size_t out_len = 0;
        moq_result_t rc = moq_codec_string_format(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_ERR_BUFFER);
        CHECK(out_len == strlen("avc1.64001e"));
        /* no partial write */
        for (size_t i = 0; i < sizeof(out); i++) CHECK(out[i] == 0xcc);
    }

    /* -- Size query (buf == NULL) ----------------------------------- */
    {
        const uint8_t avcc[] = { 0x01, 0x64, 0x00, 0x1e };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AVCC;
        cfg.sample_entry = entry("avc1");
        cfg.decoder_config = bytes(avcc, sizeof(avcc));

        size_t out_len = 0;
        moq_result_t rc = moq_codec_string_format(&cfg, NULL, 0, &out_len);
        CHECK(rc == MOQ_ERR_BUFFER);
        CHECK(out_len == strlen("avc1.64001e"));
    }

    /* -- Malformed avcC: wrong version ------------------------------ */
    {
        const uint8_t avcc[] = { 0x00, 0x64, 0x00, 0x1e };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AVCC;
        cfg.sample_entry = entry("avc1");
        cfg.decoder_config = bytes(avcc, sizeof(avcc));

        uint8_t out[32];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_string_format(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_ERR_PROTO);
    }

    /* -- Malformed avcC: too short ---------------------------------- */
    {
        const uint8_t avcc[] = { 0x01, 0x64 };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AVCC;
        cfg.sample_entry = entry("avc1");
        cfg.decoder_config = bytes(avcc, sizeof(avcc));

        uint8_t out[32];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_string_format(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_ERR_PROTO);
    }

    /* -- Truncated ASC ---------------------------------------------- */
    {
        const uint8_t asc[] = { 0x12 };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AAC_ASC;
        cfg.sample_entry = entry("mp4a");
        cfg.has_mp4_object_type_indication = true;
        cfg.mp4_object_type_indication = 0x40;
        cfg.decoder_config = bytes(asc, sizeof(asc));

        uint8_t out[32];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_string_format(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_ERR_PROTO);
    }

    /* -- Invalid args ----------------------------------------------- */
    {
        const uint8_t avcc[] = { 0x01, 0x64, 0x00, 0x1e };
        uint8_t out[32];
        size_t out_len = 0;

        /* NULL cfg / out_len */
        CHECK(moq_codec_string_format(NULL, out, sizeof(out), &out_len) == MOQ_ERR_INVAL);

        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AVCC;
        cfg.sample_entry = entry("avc1");
        cfg.decoder_config = bytes(avcc, sizeof(avcc));
        CHECK(moq_codec_string_format(&cfg, out, sizeof(out), NULL) == MOQ_ERR_INVAL);

        /* sample_entry length != 4 */
        moq_codec_string_cfg_t bad = cfg;
        bad.sample_entry = entry("avc");
        CHECK(moq_codec_string_format(&bad, out, sizeof(out), &out_len) == MOQ_ERR_INVAL);

        /* NULL decoder_config */
        bad = cfg;
        bad.decoder_config = bytes(NULL, 0);
        CHECK(moq_codec_string_format(&bad, out, sizeof(out), &out_len) == MOQ_ERR_INVAL);

        /* struct_size too small */
        bad = cfg;
        bad.struct_size = 4;
        CHECK(moq_codec_string_format(&bad, out, sizeof(out), &out_len) == MOQ_ERR_INVAL);

        /* incoherent pair: AVCC with OTI flag set */
        bad = cfg;
        bad.has_mp4_object_type_indication = true;
        CHECK(moq_codec_string_format(&bad, out, sizeof(out), &out_len) == MOQ_ERR_INVAL);

        /* incoherent pair: AAC without OTI flag */
        const uint8_t asc[] = { 0x12, 0x10 };
        bad = cfg;
        bad.config_format = MOQ_CODEC_CONFIG_AAC_ASC;
        bad.sample_entry = entry("mp4a");
        bad.decoder_config = bytes(asc, sizeof(asc));
        bad.has_mp4_object_type_indication = false;
        CHECK(moq_codec_string_format(&bad, out, sizeof(out), &out_len) == MOQ_ERR_INVAL);
    }

    /* -- Unsupported config_format ---------------------------------- */
    {
        const uint8_t cfgbytes[] = { 0x01, 0x02, 0x03, 0x04 };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = (moq_codec_config_format_t)999;
        cfg.sample_entry = entry("av01");
        cfg.decoder_config = bytes(cfgbytes, sizeof(cfgbytes));

        uint8_t out[32];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_string_format(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_ERR_UNSUPPORTED);
    }

    /* ================================================================ */
    /* init_data builder                                                */
    /* ================================================================ */

    /* -- init cfg init stamps struct_size + default nal.length_size - */
    {
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        CHECK(cfg.struct_size == sizeof(moq_codec_init_data_cfg_t));
        CHECK(cfg.nal.length_size == 4);
        CHECK(cfg.source_format == 0);
        moq_codec_init_data_cfg_init(NULL);
    }

    /* -- AVC Annex B (baseline) -> avcC ----------------------------- */
    {
        const uint8_t annexb[] = {
            0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1e, 0xab, 0xcd,
            0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x3c, 0x80,
        };
        const uint8_t expect[] = {
            0x01, 0x42, 0x00, 0x1e, 0xff, 0xe1, 0x00, 0x06,
            0x67, 0x42, 0x00, 0x1e, 0xab, 0xcd,
            0x01, 0x00, 0x04, 0x68, 0xce, 0x3c, 0x80,
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AVC_ANNEXB;
        cfg.source = bytes(annexb, sizeof(annexb));

        uint8_t out[64];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_init_data_build(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK(out_len == sizeof(expect));
        CHECK(out_len == sizeof(expect) && memcmp(out, expect, sizeof(expect)) == 0);
    }

    /* -- AVC Annex B (High profile) -> avcC with extension ---------- */
    {
        /* SPS: 67 64 00 1f AC. Byte 0xAC packs seq_id=0, chroma_format_idc=1,
         * bit_depth_luma_minus8=0, bit_depth_chroma_minus8=0. */
        const uint8_t annexb[] = {
            0x00, 0x00, 0x00, 0x01, 0x67, 0x64, 0x00, 0x1f, 0xac,
            0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x3c, 0x80,
        };
        const uint8_t expect[] = {
            0x01, 0x64, 0x00, 0x1f, 0xff, 0xe1, 0x00, 0x05,
            0x67, 0x64, 0x00, 0x1f, 0xac,
            0x01, 0x00, 0x04, 0x68, 0xce, 0x3c, 0x80,
            0xfd, 0xf8, 0xf8, 0x00, /* chroma=1, bd_luma=0, bd_chroma=0, numSPSExt=0 */
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AVC_ANNEXB;
        cfg.source = bytes(annexb, sizeof(annexb));

        uint8_t out[64];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_init_data_build(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK(out_len == sizeof(expect));
        CHECK(out_len == sizeof(expect) && memcmp(out, expect, sizeof(expect)) == 0);
    }

    /* -- Compose: build init_data then format codec string ---------- */
    {
        const uint8_t annexb[] = {
            0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1e, 0xab, 0xcd,
            0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x3c, 0x80,
        };
        moq_codec_init_data_cfg_t icfg;
        moq_codec_init_data_cfg_init(&icfg);
        icfg.source_format = MOQ_CODEC_SOURCE_AVC_ANNEXB;
        icfg.source = bytes(annexb, sizeof(annexb));

        uint8_t init_data[64];
        size_t init_len = 0;
        CHECK(moq_codec_init_data_build(&icfg, init_data, sizeof(init_data), &init_len) == MOQ_OK);

        moq_codec_string_cfg_t scfg;
        moq_codec_string_cfg_init(&scfg);
        scfg.config_format = MOQ_CODEC_CONFIG_AVCC;
        scfg.sample_entry = entry("avc1");
        scfg.decoder_config = bytes(init_data, init_len);

        uint8_t str[32];
        size_t str_len = 0;
        CHECK(moq_codec_string_format(&scfg, str, sizeof(str), &str_len) == MOQ_OK);
        CHECK_STR(str, str_len, "avc1.42001e");
    }

    /* -- AAC ASC passthrough ---------------------------------------- */
    {
        const uint8_t asc[] = { 0x12, 0x10 };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AAC_ASC;
        cfg.source = bytes(asc, sizeof(asc));

        uint8_t out[16];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_init_data_build(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK(out_len == sizeof(asc));
        CHECK(out_len == sizeof(asc) && memcmp(out, asc, sizeof(asc)) == 0);
    }

    /* -- avcC passthrough ------------------------------------------- */
    {
        const uint8_t avcc[] = {
            0x01, 0x42, 0x00, 0x1e, 0xff, 0xe1, 0x00, 0x06,
            0x67, 0x42, 0x00, 0x1e, 0xab, 0xcd,
            0x01, 0x00, 0x04, 0x68, 0xce, 0x3c, 0x80,
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AVC_AVCC;
        cfg.source = bytes(avcc, sizeof(avcc));

        uint8_t out[64];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_init_data_build(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK(out_len == sizeof(avcc));
        CHECK(out_len == sizeof(avcc) && memcmp(out, avcc, sizeof(avcc)) == 0);
    }

    /* -- Size query for the builder --------------------------------- */
    {
        const uint8_t annexb[] = {
            0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1e, 0xab, 0xcd,
            0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x3c, 0x80,
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AVC_ANNEXB;
        cfg.source = bytes(annexb, sizeof(annexb));

        size_t out_len = 0;
        moq_result_t rc = moq_codec_init_data_build(&cfg, NULL, 0, &out_len);
        CHECK(rc == MOQ_ERR_BUFFER);
        CHECK(out_len == 21);
    }

    /* -- Annex B with no SPS -> MOQ_ERR_PROTO ----------------------- */
    {
        const uint8_t annexb[] = {
            0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x3c, 0x80, /* PPS only */
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AVC_ANNEXB;
        cfg.source = bytes(annexb, sizeof(annexb));

        uint8_t out[64];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_init_data_build(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_ERR_PROTO);
    }

    /* -- Builder invalid args --------------------------------------- */
    {
        const uint8_t annexb[] = { 0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1e };
        uint8_t out[64];
        size_t out_len = 0;

        CHECK(moq_codec_init_data_build(NULL, out, sizeof(out), &out_len) == MOQ_ERR_INVAL);

        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AVC_ANNEXB;
        cfg.source = bytes(annexb, sizeof(annexb));
        CHECK(moq_codec_init_data_build(&cfg, out, sizeof(out), NULL) == MOQ_ERR_INVAL);

        /* NULL source */
        moq_codec_init_data_cfg_t bad = cfg;
        bad.source = bytes(NULL, 0);
        CHECK(moq_codec_init_data_build(&bad, out, sizeof(out), &out_len) == MOQ_ERR_INVAL);

        /* nal.length_size out of range */
        bad = cfg;
        bad.nal.length_size = 0;
        CHECK(moq_codec_init_data_build(&bad, out, sizeof(out), &out_len) == MOQ_ERR_INVAL);

        /* unsupported source_format */
        bad = cfg;
        bad.source_format = (moq_codec_source_format_t)999;
        CHECK(moq_codec_init_data_build(&bad, out, sizeof(out), &out_len) == MOQ_ERR_UNSUPPORTED);
    }

    /* ================================================================ */
    /* HEVC / AV1 codec string formatting                               */
    /* ================================================================ */

    /* -- HEVC Main profile hvcC -> hvc1.1.6.L93.b0 ------------------ */
    {
        const uint8_t hvcc[] = {
            0x01,                         /* configurationVersion */
            0x01,                         /* space=0 tier=0 profile_idc=1 */
            0x60, 0x00, 0x00, 0x00,       /* compat (reverse -> 0x6) */
            0xb0, 0x00, 0x00, 0x00, 0x00, 0x00, /* constraint flags */
            0x5d,                         /* level_idc = 93 */
        };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_HVCC;
        cfg.sample_entry = entry("hvc1");
        cfg.decoder_config = bytes(hvcc, sizeof(hvcc));

        uint8_t out[64];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_string_format(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK_STR(out, out_len, "hvc1.1.6.L93.b0");
    }

    /* -- HEVC profile_space + High tier -> hev1.B4.8.H120.90 -------- */
    {
        const uint8_t hvcc[] = {
            0x01,
            0xa4,                         /* space=2 tier=1 profile_idc=4 */
            0x10, 0x00, 0x00, 0x00,       /* compat (reverse -> 0x8) */
            0x90, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x78,                         /* level_idc = 120 */
        };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_HVCC;
        cfg.sample_entry = entry("hev1");
        cfg.decoder_config = bytes(hvcc, sizeof(hvcc));

        uint8_t out[64];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_string_format(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK_STR(out, out_len, "hev1.B4.8.H120.90");
    }

    /* -- HEVC truncated hvcC -> MOQ_ERR_PROTO ----------------------- */
    {
        const uint8_t hvcc[] = { 0x01, 0x01, 0x60 };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_HVCC;
        cfg.sample_entry = entry("hvc1");
        cfg.decoder_config = bytes(hvcc, sizeof(hvcc));

        uint8_t out[64];
        size_t out_len = 0;
        CHECK(moq_codec_string_format(&cfg, out, sizeof(out), &out_len) == MOQ_ERR_PROTO);
    }

    /* -- AV1 profile 0 av1C -> av01.0.04M.08 ------------------------ */
    {
        const uint8_t av1c[] = {
            0x81,       /* marker=1 version=1 */
            0x04,       /* seq_profile=0 seq_level_idx=4 */
            0x00,       /* tier=0 8-bit */
            0x00,
        };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AV1C;
        cfg.sample_entry = entry("av01");
        cfg.decoder_config = bytes(av1c, sizeof(av1c));

        uint8_t out[64];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_string_format(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK_STR(out, out_len, "av01.0.04M.08");
    }

    /* -- AV1 profile 2, High tier, 12-bit -> av01.2.08H.12 ---------- */
    {
        const uint8_t av1c[] = {
            0x81,
            0x48,       /* seq_profile=2 seq_level_idx=8 */
            0xe0,       /* tier=1 high_bitdepth=1 twelve_bit=1 */
            0x00,
        };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AV1C;
        cfg.sample_entry = entry("av01");
        cfg.decoder_config = bytes(av1c, sizeof(av1c));

        uint8_t out[64];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_string_format(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK_STR(out, out_len, "av01.2.08H.12");
    }

    /* -- AV1 bad marker/version -> MOQ_ERR_PROTO -------------------- */
    {
        const uint8_t av1c[] = { 0x82, 0x04, 0x00, 0x00 }; /* version=2 */
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AV1C;
        cfg.sample_entry = entry("av01");
        cfg.decoder_config = bytes(av1c, sizeof(av1c));

        uint8_t out[64];
        size_t out_len = 0;
        CHECK(moq_codec_string_format(&cfg, out, sizeof(out), &out_len) == MOQ_ERR_PROTO);
    }

    /* ================================================================ */
    /* HEVC / AV1 init_data passthrough + unsupported builders          */
    /* ================================================================ */

    /* -- hvcC passthrough ------------------------------------------- */
    {
        uint8_t hvcc[23];
        memset(hvcc, 0, sizeof(hvcc));
        hvcc[0] = 0x01;
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_HEVC_HVCC;
        cfg.source = bytes(hvcc, sizeof(hvcc));

        uint8_t out[32];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_init_data_build(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK(out_len == sizeof(hvcc));
        CHECK(out_len == sizeof(hvcc) && memcmp(out, hvcc, sizeof(hvcc)) == 0);
    }

    /* -- hvcC too short -> MOQ_ERR_PROTO ---------------------------- */
    {
        uint8_t hvcc[10];
        memset(hvcc, 0, sizeof(hvcc));
        hvcc[0] = 0x01;
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_HEVC_HVCC;
        cfg.source = bytes(hvcc, sizeof(hvcc));

        uint8_t out[32];
        size_t out_len = 0;
        CHECK(moq_codec_init_data_build(&cfg, out, sizeof(out), &out_len) == MOQ_ERR_PROTO);
    }

    /* -- av1C passthrough ------------------------------------------- */
    {
        /* Four record bytes, then one configOBU: header 0x0a is a
         * sequence header with obu_has_size_field set, and the LEB128
         * size 0x00 gives it a zero-length payload. */
        const uint8_t av1c[] = { 0x81, 0x04, 0x00, 0x00, 0x0a, 0x00 };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AV1_AV1C;
        cfg.source = bytes(av1c, sizeof(av1c));

        uint8_t out[32];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_init_data_build(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK(out_len == sizeof(av1c));
        CHECK(out_len == sizeof(av1c) && memcmp(out, av1c, sizeof(av1c)) == 0);
    }

    /* -- HEVC Annex B -> hvcC (byte-for-byte vs ffmpeg/x265) -------- */
    {
        /* VPS + SPS + PPS produced by x265 (info=0), Annex B. */
        static const uint8_t annexb[] = {
            0x00, 0x00, 0x00, 0x01, 0x40, 0x01, 0x0c, 0x01, 0xff, 0xff, 0x04, 0x08,
            0x00, 0x00, 0x03, 0x00, 0x9e, 0x08, 0x00, 0x00, 0x03, 0x00, 0x00, 0x1e,
            0x95, 0x98, 0x09, 0x00, 0x00, 0x00, 0x01, 0x42, 0x01, 0x01, 0x04, 0x08,
            0x00, 0x00, 0x03, 0x00, 0x9e, 0x08, 0x00, 0x00, 0x03, 0x00, 0x00, 0x1e,
            0x90, 0x04, 0x10, 0x20, 0xb2, 0xca, 0xcd, 0x24, 0x99, 0x5e, 0x02, 0xdc,
            0x08, 0x08, 0x00, 0x10, 0x00, 0x00, 0x03, 0x00, 0x10, 0x00, 0x00, 0x03,
            0x00, 0x10, 0x80, 0x00, 0x00, 0x00, 0x01, 0x44, 0x01, 0xc1, 0x72, 0x86,
            0x0c, 0x42, 0x24,
        };
        /* The hvcC ffmpeg writes into the mp4 stsd for the same input. */
        static const uint8_t expect[] = {
            0x01, 0x04, 0x08, 0x00, 0x00, 0x00, 0x9e, 0x08, 0x00, 0x00, 0x00, 0x00,
            0x1e, 0xf0, 0x00, 0xfc, 0xff, 0xf8, 0xf8, 0x00, 0x00, 0x0f, 0x03, 0xa0,
            0x00, 0x01, 0x00, 0x17, 0x40, 0x01, 0x0c, 0x01, 0xff, 0xff, 0x04, 0x08,
            0x00, 0x00, 0x03, 0x00, 0x9e, 0x08, 0x00, 0x00, 0x03, 0x00, 0x00, 0x1e,
            0x95, 0x98, 0x09, 0xa1, 0x00, 0x01, 0x00, 0x2c, 0x42, 0x01, 0x01, 0x04,
            0x08, 0x00, 0x00, 0x03, 0x00, 0x9e, 0x08, 0x00, 0x00, 0x03, 0x00, 0x00,
            0x1e, 0x90, 0x04, 0x10, 0x20, 0xb2, 0xca, 0xcd, 0x24, 0x99, 0x5e, 0x02,
            0xdc, 0x08, 0x08, 0x00, 0x10, 0x00, 0x00, 0x03, 0x00, 0x10, 0x00, 0x00,
            0x03, 0x00, 0x10, 0x80, 0xa2, 0x00, 0x01, 0x00, 0x08, 0x44, 0x01, 0xc1,
            0x72, 0x86, 0x0c, 0x42, 0x24,
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_HEVC_ANNEXB;
        cfg.source = bytes(annexb, sizeof(annexb));

        uint8_t out[128];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_init_data_build(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK(out_len == sizeof(expect));
        CHECK(out_len == sizeof(expect) && memcmp(out, expect, sizeof(expect)) == 0);
    }

    /* -- HEVC Annex B with temporal sub-layers -> hvcC -------------- */
    {
        /* VPS + SPS + PPS captured from Apple's low-latency HEVC encoder
         * (com.apple.videotoolbox.videoencoder.hevc.rtvc) at 1280x720. Its SPS
         * carries sps_max_sub_layers_minus1 = 1, which the parser used to
         * reject outright with MOQ_ERR_UNSUPPORTED -- no hvcC meant no video
         * track was ever announced, so a stream encoded this way came out
         * audio-only. The sub-layer PTL is skipped, not interpreted. */
        static const uint8_t annexb[] = {
            0x00, 0x00, 0x00, 0x01, 0x40, 0x01, 0x0c, 0x03, 0xff, 0xff, 0x01, 0x60,
            0x00, 0x00, 0x03, 0x00, 0xb0, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00,
            0x5d, 0x00, 0x00, 0x1b, 0x02, 0x40, 0x00, 0x00, 0x00, 0x01, 0x42, 0x01,
            0x03, 0x01, 0x60, 0x00, 0x00, 0x03, 0x00, 0xb0, 0x00, 0x00, 0x03, 0x00,
            0x00, 0x03, 0x00, 0x5d, 0x00, 0x00, 0xa0, 0x02, 0x80, 0x80, 0x2d, 0x16,
            0x20, 0x6e, 0xe4, 0x52, 0x32, 0xe7, 0xe1, 0x3d, 0x0b, 0xea, 0x1b, 0xd5,
            0x29, 0xa8, 0x10, 0x10, 0x10, 0x1f, 0xc2, 0x01, 0x04, 0x00, 0x00, 0x00,
            0x01, 0x44, 0x01, 0xc0, 0x72, 0xf0, 0x5b, 0x24,
        };
        /* general profile_tier_level, hvcC bytes 1..12: Main profile, Main
         * tier, general_level_idc 0x5d (level 4.0). */
        static const uint8_t ptl[] = {
            0x01, 0x60, 0x00, 0x00, 0x00, 0xb0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x5d,
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_HEVC_ANNEXB;
        cfg.source = bytes(annexb, sizeof(annexb));

        uint8_t out[256];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_init_data_build(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK(out_len >= 23);
        CHECK(out[0] == 0x01);                            /* configurationVersion */
        CHECK(memcmp(out + 1, ptl, sizeof(ptl)) == 0);
        /* byte 21: constantFrameRate(2)=0 | numTemporalLayers(3)=2 |
         * temporalIdNested(1)=1 | lengthSizeMinusOne(2)=3 */
        CHECK(out[21] == 0x17);
    }

    /* -- AV1 OBU -> av1C (header vs ffmpeg; seq header verbatim) ---- */
    {
        /* Temporal delimiter + sequence header OBU from SVT-AV1. */
        static const uint8_t obu[] = {
            0x12, 0x00, 0x0a, 0x0a, 0x00, 0x00, 0x00, 0x02, 0xaf, 0xff, 0x8d, 0x5f,
            0x30, 0x08,
        };
        /* ffmpeg's av1C header (81 00 0c 00) followed by the sequence header
         * OBU copied verbatim. ffmpeg re-normalizes the OBU (its configOBUs
         * end ...80 5f 00 08); a verbatim copy is equally conformant. */
        static const uint8_t expect[] = {
            0x81, 0x00, 0x0c, 0x00, 0x0a, 0x0a, 0x00, 0x00, 0x00, 0x02, 0xaf, 0xff,
            0x8d, 0x5f, 0x30, 0x08,
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AV1_OBU;
        cfg.source = bytes(obu, sizeof(obu));

        uint8_t out[32];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_init_data_build(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK(out_len == sizeof(expect));
        CHECK(out_len == sizeof(expect) && memcmp(out, expect, sizeof(expect)) == 0);
        /* the four header bytes match ffmpeg's av1C exactly */
        static const uint8_t ff_header[] = { 0x81, 0x00, 0x0c, 0x00 };
        CHECK(memcmp(out, ff_header, 4) == 0);
    }

    /* -- No sequence header in OBU stream -> MOQ_ERR_PROTO ---------- */
    {
        const uint8_t obu[] = { 0x12, 0x00 }; /* temporal delimiter only */
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AV1_OBU;
        cfg.source = bytes(obu, sizeof(obu));

        uint8_t out[32];
        size_t out_len = 0;
        CHECK(moq_codec_init_data_build(&cfg, out, sizeof(out), &out_len) == MOQ_ERR_PROTO);
    }

    /* -- HEVC Annex B with no SPS -> MOQ_ERR_PROTO ------------------ */
    {
        const uint8_t annexb[] = {
            0x00, 0x00, 0x00, 0x01, 0x44, 0x01, 0xc1, 0x72, /* PPS only */
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_HEVC_ANNEXB;
        cfg.source = bytes(annexb, sizeof(annexb));

        uint8_t out[128];
        size_t out_len = 0;
        CHECK(moq_codec_init_data_build(&cfg, out, sizeof(out), &out_len) == MOQ_ERR_PROTO);
    }

    /* -- HEVC builder honours the size-query protocol --------------- */
    {
        static const uint8_t annexb[] = {
            0x00, 0x00, 0x00, 0x01, 0x40, 0x01, 0x0c, 0x01, 0xff, 0xff, 0x04, 0x08,
            0x00, 0x00, 0x03, 0x00, 0x9e, 0x08, 0x00, 0x00, 0x03, 0x00, 0x00, 0x1e,
            0x95, 0x98, 0x09, 0x00, 0x00, 0x00, 0x01, 0x42, 0x01, 0x01, 0x04, 0x08,
            0x00, 0x00, 0x03, 0x00, 0x9e, 0x08, 0x00, 0x00, 0x03, 0x00, 0x00, 0x1e,
            0x90, 0x04, 0x10, 0x20, 0xb2, 0xca, 0xcd, 0x24, 0x99, 0x5e, 0x02, 0xdc,
            0x08, 0x08, 0x00, 0x10, 0x00, 0x00, 0x03, 0x00, 0x10, 0x00, 0x00, 0x03,
            0x00, 0x10, 0x80, 0x00, 0x00, 0x00, 0x01, 0x44, 0x01, 0xc1, 0x72, 0x86,
            0x0c, 0x42, 0x24,
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_HEVC_ANNEXB;
        cfg.source = bytes(annexb, sizeof(annexb));

        size_t out_len = 0;
        CHECK(moq_codec_init_data_build(&cfg, NULL, 0, &out_len) == MOQ_ERR_BUFFER);
        CHECK(out_len == 113);
    }

    /* ================================================================ */
    /* Opus codec string + init_data                                    */
    /* ================================================================ */

    /* -- Opus codec string is just "opus" --------------------------- */
    {
        const uint8_t dops[] = {
            0x00, 0x02, 0x01, 0x38, 0x00, 0x00, 0xbb, 0x80, 0x00, 0x00, 0x00,
        };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_OPUS;
        cfg.sample_entry = entry("opus");
        cfg.decoder_config = bytes(dops, sizeof(dops));

        uint8_t out[32];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_string_format(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK_STR(out, out_len, "opus");
    }

    /* -- Opus with OTI flag set is incoherent ----------------------- */
    {
        const uint8_t dops[] = {
            0x00, 0x02, 0x01, 0x38, 0x00, 0x00, 0xbb, 0x80, 0x00, 0x00, 0x00,
        };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_OPUS;
        cfg.sample_entry = entry("opus");
        cfg.has_mp4_object_type_indication = true;
        cfg.decoder_config = bytes(dops, sizeof(dops));

        uint8_t out[32];
        size_t out_len = 0;
        CHECK(moq_codec_string_format(&cfg, out, sizeof(out), &out_len) == MOQ_ERR_INVAL);
    }

    /* -- OpusHead -> dOps (strip magic, LE->BE, version 0) ---------- */
    {
        const uint8_t opus_head[] = {
            'O', 'p', 'u', 's', 'H', 'e', 'a', 'd', /* magic */
            0x01,                   /* version = 1 */
            0x02,                   /* channel count = 2 */
            0x38, 0x01,             /* pre-skip = 312 (LE) */
            0x80, 0xbb, 0x00, 0x00, /* input sample rate = 48000 (LE) */
            0x00, 0x00,             /* output gain = 0 */
            0x00,                   /* channel mapping family = 0 */
        };
        const uint8_t expect[] = {
            0x00,                   /* version = 0 */
            0x02,                   /* channel count = 2 */
            0x01, 0x38,             /* pre-skip = 312 (BE) */
            0x00, 0x00, 0xbb, 0x80, /* input sample rate = 48000 (BE) */
            0x00, 0x00,             /* output gain = 0 */
            0x00,                   /* channel mapping family = 0 */
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_OPUS_HEAD;
        cfg.source = bytes(opus_head, sizeof(opus_head));

        uint8_t out[32];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_init_data_build(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK(out_len == sizeof(expect));
        CHECK(out_len == sizeof(expect) && memcmp(out, expect, sizeof(expect)) == 0);
    }

    /* -- dOps passthrough ------------------------------------------- */
    {
        const uint8_t dops[] = {
            0x00, 0x02, 0x01, 0x38, 0x00, 0x00, 0xbb, 0x80, 0x00, 0x00, 0x00,
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_OPUS_DOPS;
        cfg.source = bytes(dops, sizeof(dops));

        uint8_t out[32];
        size_t out_len = 0;
        moq_result_t rc = moq_codec_init_data_build(&cfg, out, sizeof(out), &out_len);
        CHECK(rc == MOQ_OK);
        CHECK(out_len == sizeof(dops));
        CHECK(out_len == sizeof(dops) && memcmp(out, dops, sizeof(dops)) == 0);
    }

    /* -- OpusHead without magic -> MOQ_ERR_PROTO -------------------- */
    {
        const uint8_t bad[] = {
            'N', 'o', 't', 'H', 'e', 'a', 'd', '!', 0x01, 0x02,
            0x00, 0x00, 0x80, 0xbb, 0x00, 0x00, 0x00, 0x00, 0x00,
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_OPUS_HEAD;
        cfg.source = bytes(bad, sizeof(bad));

        uint8_t out[32];
        size_t out_len = 0;
        CHECK(moq_codec_init_data_build(&cfg, out, sizeof(out), &out_len) == MOQ_ERR_PROTO);
    }

    if (failures == 0) printf("PASS: test_codec_signaling\n");
    return failures;
}
