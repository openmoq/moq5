#include <moq/cmaf_packager.h>
#include <moq/cmaf.h>
#include <moq/codec_signaling.h>
#include "codec_asc_internal.h"
#include <stdio.h>
#include <string.h>

static int failures;
#define CHECK(x) do { if (!(x)) { \
    fprintf(stderr, "FAIL:%d: %s\n", __LINE__, #x); failures++; } } while (0)

static void bits(uint8_t *out, size_t *pos, uint32_t value, unsigned n)
{
    for (unsigned i = 0; i < n; i++, (*pos)++)
        if ((value >> (n - i - 1)) & 1)
            out[*pos / 8] |= (uint8_t)(1u << (7 - (*pos & 7)));
}

static void shared_asc_cases(void)
{
    /* Common-field vectors, not claims that AOT-specific/PCE syntax is fully
     * valid. Existing public utilities deliberately validate only the prefix. */
    const uint32_t aots[] = {2, 2, 42, 2, 5, 29, 2, 2, 2, 2, 2};
    for (size_t i = 0; i < sizeof(aots) / sizeof(aots[0]); i++) {
        uint8_t record[16] = {0};
        size_t pos = 0;
        uint32_t aot = aots[i], frequency = i == 1 ? 15 : i == 4 || i == 5 || i == 6 ? 6 : 3;
        uint32_t channels = i == 3 ? 0 : 2;
        bits(record, &pos, aot >= 32 ? 31 : aot, 5);
        if (aot >= 32) bits(record, &pos, aot - 32, 6);
        if (i == 7) frequency = 13; /* unknown/reserved index acceptance preserved */
        bits(record, &pos, frequency, 4);
        if (frequency == 15) bits(record, &pos, 48000, 24);
        bits(record, &pos, channels, 4);
        if (aot == 5 || aot == 29) {
            bits(record, &pos, 3, 4); /* extension output frequency differs */
            bits(record, &pos, 2, 5);
        }
        /* dependsOnCoreCoder/extensionFlag require uninspected syntax. */
        bits(record, &pos, i == 8 ? 2 : i == 9 ? 1 : 0, 3);
        if (i == 10) {
            /* Ordinary GA ends on a byte boundary. A further nonzero byte
             * cannot be certified as padding rather than an extension tail. */
            bits(record, &pos, 1, 8);
        }
        if (i == 6) {
            bits(record, &pos, 0x2b7, 11); /* explicit sync extension tail */
            bits(record, &pos, 5, 5);
            bits(record, &pos, 1, 1);
            bits(record, &pos, 3, 4);
        }
        size_t len = (pos + 7) / 8;
        codec_asc_info_t info;
        CHECK(codec_asc_inspect(record, len, &info) == MOQ_OK);
        CHECK(info.object_type == aot && info.frequency_index == frequency);
        CHECK(info.channel_configuration == channels);
        CHECK(info.declared_frequency_known == (i < 2));
        CHECK(info.declared_channels_known == (i < 2 || i == 7));
        if (i < 2) CHECK(info.frequency == 48000 && info.declared_channels == 2);

        moq_codec_init_data_cfg_t build;
        moq_codec_init_data_cfg_init(&build);
        build.source_format = MOQ_CODEC_SOURCE_AAC_ASC;
        build.source = (moq_bytes_t){record, len};
        uint8_t copy[16]; size_t required = 0;
        CHECK(moq_codec_init_data_build(&build, copy, sizeof(copy), &required) == MOQ_OK);
        CHECK(required == len && memcmp(copy, record, len) == 0);
        moq_codec_string_cfg_t format;
        moq_codec_string_cfg_init(&format);
        format.config_format = MOQ_CODEC_CONFIG_AAC_ASC;
        format.sample_entry = MOQ_BYTES_LITERAL("mp4a");
        format.decoder_config = build.source;
        format.has_mp4_object_type_indication = true;
        format.mp4_object_type_indication = 0x40;
        uint8_t string[32]; char expected[32];
        int n = snprintf(expected, sizeof(expected), "mp4a.40.%u", aot);
        CHECK(moq_codec_string_format(&format, string, sizeof(string), &required) == MOQ_OK);
        CHECK(n > 0 && required == (size_t)n && memcmp(string, expected, required) == 0);

        moq_cmaf_packager_cfg_t cfg;
        moq_cmaf_packager_cfg_init(&cfg);
        cfg.sample_entry = MOQ_CMAF_ENTRY_MP4A;
        cfg.sample_format = MOQ_CMAF_SAMPLE_AAC_RAW;
        cfg.codec_config = build.source;
        cfg.samplerate = 48000; cfg.channel_count = 2;
        moq_cmaf_packager_t *p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_OK);
        if (p) moq_cmaf_packager_destroy(p);
        cfg.samplerate = 44100;
        cfg.channel_count = i == 7 ? 2 : 1;
        moq_result_t rc = moq_cmaf_packager_create(NULL, &cfg, &p);
        CHECK(rc == (i < 2 ? MOQ_ERR_INVAL : MOQ_OK));
        if (p) moq_cmaf_packager_destroy(p);
    }
}

static void check_audio(bool opus)
{
    const uint8_t asc[] = {0x11, 0x90}; /* 48 kHz, stereo */
    uint8_t dops[] = {0, 2, 1, 0x38, 0, 0, 0xbb, 0x80, 0, 0, 0};
    moq_cmaf_packager_cfg_t cfg;
    moq_cmaf_packager_cfg_init(&cfg);
    cfg.sample_entry = opus ? MOQ_CMAF_ENTRY_OPUS : MOQ_CMAF_ENTRY_MP4A;
    cfg.sample_format = opus ? MOQ_CMAF_SAMPLE_OPUS_PACKET : MOQ_CMAF_SAMPLE_AAC_RAW;
    cfg.codec_config = opus ? (moq_bytes_t){dops, sizeof(dops)}
                            : (moq_bytes_t){asc, sizeof(asc)};
    cfg.samplerate = 48000;
    cfg.channel_count = 2;
    moq_cmaf_packager_t *p = NULL;
    CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_OK);
    if (p) {
        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init_sized(moq_cmaf_packager_init_segment(p), &info, sizeof(info)) == MOQ_OK);
        CHECK(info.samplerate == 48000 && info.channel_count == 2);
        moq_cmaf_packager_destroy(p);
    }
    /* Change channels alone, then (AAC only) rate alone, then both. Each
     * mismatch must refuse before publishing a contradictory init segment. */
    cfg.channel_count = 1;
    moq_result_t rc = moq_cmaf_packager_create(NULL, &cfg, &p);
    printf("%s stereo-config/mono-cfg rc=%d\n", opus ? "Opus" : "AAC", rc);
    CHECK(rc == MOQ_ERR_INVAL && p == NULL);
    if (p) moq_cmaf_packager_destroy(p);
    if (!opus) {
        cfg.channel_count = 2; cfg.samplerate = 44100;
        rc = moq_cmaf_packager_create(NULL, &cfg, &p);
        printf("AAC 48k-config/44100-cfg rc=%d\n", rc);
        CHECK(rc == MOQ_ERR_INVAL && p == NULL);
        if (p) moq_cmaf_packager_destroy(p);
        cfg.channel_count = 1;
        rc = moq_cmaf_packager_create(NULL, &cfg, &p);
        printf("AAC stereo48k-config/mono44100-cfg rc=%d\n", rc);
        CHECK(rc == MOQ_ERR_INVAL && p == NULL);
        if (p) moq_cmaf_packager_destroy(p);
    } else {
        dops[1] = 1; /* matching mono is legal, not a blanket stereo gate */
        CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_OK);
        if (p) moq_cmaf_packager_destroy(p);
        dops[1] = 0;
        CHECK(moq_cmaf_packager_create(NULL, &cfg, &p) == MOQ_ERR_PROTO && p == NULL);
        if (p) moq_cmaf_packager_destroy(p);
    }
}
int main(int argc, char **argv)
{
    if (argc != 2) return 2;
    if (strcmp(argv[1], "aac") == 0) { check_audio(false); shared_asc_cases(); }
    else if (strcmp(argv[1], "opus") == 0) check_audio(true);
    else return 2;
    printf("audio metadata failures=%d\n", failures);
    return failures ? 1 : 0;
}
