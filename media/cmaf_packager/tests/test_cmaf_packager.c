#include <moq/cmaf_packager.h>
#include <moq/cmaf.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures;
#define CHECK(x) do { if (!(x)) { \
    fprintf(stderr, "FAIL:%d: %s\n", __LINE__, #x); failures++; } } while (0)
static const uint8_t avcc[] = {
    1, 0x64, 0, 0x1f, 0xff, 0xe1, 0, 4, 0x67, 0x64, 0, 0x1f,
    1, 0, 2, 0x68, 0xee
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
static const uint8_t nal[] = { 0, 0, 1, 0x65, 0x11 };

typedef struct {
    moq_cmaf_sample_entry_t entry;
    moq_cmaf_sample_format_t format;
    const uint8_t *config;
    size_t config_len;
    const char *box;
    moq_cmaf_codec_kind_t parser_kind;
} codec_t;
static const codec_t codecs[] = {
    {MOQ_CMAF_ENTRY_AVC1, MOQ_CMAF_SAMPLE_NAL_ANNEXB, avcc, sizeof(avcc), "avc1", MOQ_CMAF_CODEC_AVC},
    {MOQ_CMAF_ENTRY_HVC1, MOQ_CMAF_SAMPLE_NAL_ANNEXB, hvcc, sizeof(hvcc), "hvc1", MOQ_CMAF_CODEC_HEVC},
    {MOQ_CMAF_ENTRY_MP4A, MOQ_CMAF_SAMPLE_AAC_RAW, asc, sizeof(asc), "mp4a", MOQ_CMAF_CODEC_AAC},
    {MOQ_CMAF_ENTRY_AV01, MOQ_CMAF_SAMPLE_AV1_OBU, av1c, sizeof(av1c), "av01", MOQ_CMAF_CODEC_AV1},
    {MOQ_CMAF_ENTRY_OPUS, MOQ_CMAF_SAMPLE_OPUS_PACKET, dops, sizeof(dops), "Opus", MOQ_CMAF_CODEC_OPUS}
};
static moq_cmaf_packager_cfg_t config(size_t i)
{
    moq_cmaf_packager_cfg_t c;
    moq_cmaf_packager_cfg_init(&c);
    c.sample_entry = codecs[i].entry;
    c.sample_format = codecs[i].format;
    c.codec_config = (moq_bytes_t){codecs[i].config, codecs[i].config_len};
    c.width = 640; c.height = 480;
    c.samplerate = 48000; c.channel_count = 2;
    c.timescale = i < 2 || i == 3 ? 90000 : 0;
    return c;
}
static uint32_t rd32(const uint8_t *p)
{
    return (uint32_t)p[0] << 24 | (uint32_t)p[1] << 16 |
           (uint32_t)p[2] << 8 | p[3];
}
/* Only descend into known ordinary containers. Leaf bytes never become boxes. */
static moq_bytes_t box(moq_bytes_t bytes, const char *type)
{
    size_t offset = 0;
    while (bytes.len - offset >= 8) {
        const uint8_t *p = bytes.data + offset;
        uint32_t n = rd32(p);
        if (n < 8 || n > bytes.len - offset) break;
        if (memcmp(p + 4, type, 4) == 0) return (moq_bytes_t){p + 8, n - 8};
        if (memcmp(p + 4, "moof", 4) == 0 || memcmp(p + 4, "traf", 4) == 0 ||
            memcmp(p + 4, "moov", 4) == 0 || memcmp(p + 4, "trak", 4) == 0 ||
            memcmp(p + 4, "mdia", 4) == 0 || memcmp(p + 4, "minf", 4) == 0 ||
            memcmp(p + 4, "stbl", 4) == 0) {
            moq_bytes_t found = box((moq_bytes_t){p + 8, n - 8}, type);
            if (found.data) return found;
        }
        offset += n;
    }
    return (moq_bytes_t){NULL, 0};
}
static uint32_t sequence(moq_bytes_t frag)
{
    moq_bytes_t mfhd = box(frag, "mfhd");
    CHECK(mfhd.len == 8);
    return mfhd.len == 8 ? rd32(mfhd.data + 4) : 0;
}
static uint64_t decode_time(moq_bytes_t frag)
{
    moq_bytes_t tfdt = box(frag, "tfdt");
    CHECK(tfdt.len == 12 && tfdt.data[0] == 1);
    return tfdt.len == 12 ? (uint64_t)rd32(tfdt.data + 4) << 32 |
                           rd32(tfdt.data + 8) : UINT64_MAX;
}
static moq_cmaf_packager_sample_t sample(int64_t dts)
{
    moq_cmaf_packager_sample_t s = {
        .data = {nal, sizeof(nal)}, .pts = dts, .dts = dts,
        .duration = 3000, .keyframe = true
    };
    return s;
}
static void five_codecs(void)
{
    static const uint8_t hevc[] = {0, 0, 1, 0x26, 1, 0x11};
    static const uint8_t raw[] = {0x11, 0x22, 0x33};
    static const uint8_t expected_avc[] = {0, 0, 0, 2, 0x65, 0x11};
    static const uint8_t expected_hevc[] = {0, 0, 0, 3, 0x26, 1, 0x11};
    for (size_t i = 0; i < sizeof(codecs) / sizeof(codecs[0]); i++) {
        moq_cmaf_packager_cfg_t c = config(i);
        moq_cmaf_packager_t *p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &c, &p) == MOQ_OK);
        if (!p) continue;
        moq_bytes_t init = moq_cmaf_packager_init_segment(p);
        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init_sized(init, &info, sizeof(info)) == MOQ_OK);
        CHECK(info.codec_kind == codecs[i].parser_kind);
        CHECK(info.track_id == 1);
        CHECK(info.timescale == moq_cmaf_packager_timescale(p));
        CHECK(info.codec_config.len == codecs[i].config_len);
        if (info.codec_config.len == codecs[i].config_len)
            CHECK(memcmp(info.codec_config.data, codecs[i].config, info.codec_config.len) == 0);
        moq_bytes_t stsd = box(init, "stsd");
        CHECK(stsd.len >= 16);
        if (stsd.len >= 16) CHECK(memcmp(stsd.data + 12, codecs[i].box, 4) == 0);
        moq_cmaf_packager_sample_t s = sample(9000);
        if (i == 1) s.data = (moq_bytes_t){hevc, sizeof(hevc)};
        if (i >= 2) s.data = (moq_bytes_t){raw, sizeof(raw)};
        s.pts += 7;
        moq_bytes_t frag;
        CHECK(moq_cmaf_packager_write(p, &s, 1, &frag) == MOQ_OK);
        CHECK(sequence(frag) == 1 && decode_time(frag) == 9000);
        uint32_t moof_len = rd32(frag.data);
        CHECK(moof_len <= frag.len && frag.len - moof_len >= 8);
        moq_bytes_t trun = box(frag, "trun");
        CHECK(trun.len >= 12);
        if (trun.len >= 12) CHECK(rd32(trun.data + 8) == moof_len + 8);
        moq_bytes_t mdat = box(frag, "mdat");
        const uint8_t *expected = i == 0 ? expected_avc : i == 1 ? expected_hevc : raw;
        size_t expected_len = i == 0 ? sizeof(expected_avc) : i == 1 ? sizeof(expected_hevc) : sizeof(raw);
        CHECK(mdat.len == expected_len);
        if (mdat.len == expected_len) CHECK(memcmp(mdat.data, expected, expected_len) == 0);
        moq_cmaf_sample_t table[2];
        moq_cmaf_fragment_info_t fi;
        moq_cmaf_fragment_info_init(&fi, table, 2);
        CHECK(moq_cmaf_parse_fragment(frag, &fi) == MOQ_OK);
        CHECK(fi.sample_count == 1 && table[0].duration == 3000);
        CHECK(table[0].composition_offset == 7);
        CHECK(moq_cmaf_packager_init_segment(p).data == init.data);
        moq_cmaf_packager_destroy(p);
    }
}
static void timing_and_aliases(void)
{
    moq_cmaf_packager_cfg_t c = config(0);
    moq_cmaf_packager_t *p = NULL;
    CHECK(moq_cmaf_packager_create(NULL, &c, &p) == MOQ_OK);
    if (!p) return;
    moq_cmaf_packager_sample_t s[2] = {sample(0), sample(3000)};
    s[1].pts = 2900;
    moq_bytes_t out;
    CHECK(moq_cmaf_packager_write(p, s, 2, &out) == MOQ_OK);
    CHECK(sequence(out) == 1);
    moq_bytes_t trun = box(out, "trun");
    CHECK(trun.len >= 4 && trun.data[0] == 1);
    moq_cmaf_packager_sample_t alias = sample(6000);
    alias.data = out;
    CHECK(moq_cmaf_packager_write(p, &alias, 1, &out) == MOQ_ERR_INVAL);
    CHECK(out.data == NULL && out.len == 0);
    alias.data = moq_cmaf_packager_init_segment(p);
    CHECK(moq_cmaf_packager_write(p, &alias, 1, &out) == MOQ_ERR_INVAL);
    alias = sample(5999);
    CHECK(moq_cmaf_packager_write(p, &alias, 1, &out) == MOQ_ERR_INVAL);
    alias = sample(9000); /* cross-write gap preserved */
    CHECK(moq_cmaf_packager_write(p, &alias, 1, &out) == MOQ_OK);
    CHECK(sequence(out) == 2 && decode_time(out) == 9000);
    alias = sample(INT64_MAX);
    CHECK(moq_cmaf_packager_write(p, &alias, 1, &out) == MOQ_ERR_INVAL);
    alias = sample(12000); alias.pts += (int64_t)INT32_MAX + 1;
    CHECK(moq_cmaf_packager_write(p, &alias, 1, &out) == MOQ_ERR_INVAL);
    alias = sample(12000); alias.duration = 0;
    CHECK(moq_cmaf_packager_write(p, &alias, 1, &out) == MOQ_ERR_INVAL);
    alias = sample(12000);
    CHECK(moq_cmaf_packager_write(p, &alias, 1, &out) == MOQ_OK);
    CHECK(sequence(out) == 3);
    moq_cmaf_packager_destroy(p);
    c.rebase_timestamps = true;
    c.source_timebase_num = 1; c.source_timebase_den = 1000000;
    CHECK(moq_cmaf_packager_create(NULL, &c, &p) == MOQ_OK);
    if (!p) return;
    alias = sample(-5000000);
    CHECK(moq_cmaf_packager_write(p, &alias, 1, &out) == MOQ_OK);
    CHECK(decode_time(out) == 0);
    alias = sample(-4900000);
    CHECK(moq_cmaf_packager_write(p, &alias, 1, &out) == MOQ_OK);
    CHECK(decode_time(out) == 9000);
    moq_cmaf_packager_destroy(p);
}
static void aac_formats(void)
{
    const uint8_t adts[] = {0xff, 0xf1, 0x4c, 0x80, 0x01, 0x5f, 0xfc, 0x11, 0x22, 0x33};
    for (int raw = 0; raw < 2; raw++) {
        moq_cmaf_packager_cfg_t c = config(2);
        c.sample_format = raw ? MOQ_CMAF_SAMPLE_AAC_RAW : MOQ_CMAF_SAMPLE_AAC_ADTS;
        moq_cmaf_packager_t *p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &c, &p) == MOQ_OK);
        if (!p) continue;
        moq_cmaf_packager_sample_t s = sample(0);
        s.data = (moq_bytes_t){adts, sizeof(adts)}; s.duration = 1024;
        moq_bytes_t out;
        CHECK(moq_cmaf_packager_write(p, &s, 1, &out) == MOQ_OK);
        moq_bytes_t mdat = box(out, "mdat");
        size_t skip = raw ? 0 : 7;
        CHECK(mdat.len == sizeof(adts) - skip);
        if (mdat.len == sizeof(adts) - skip) CHECK(memcmp(mdat.data, adts + skip, mdat.len) == 0);
        if (!raw) {
            uint8_t bad[sizeof(adts)]; memcpy(bad, adts, sizeof(bad));
            bad[2] ^= 4; /* a different sampling frequency */
            s = sample(1024); s.duration = 1024; s.data = (moq_bytes_t){bad, sizeof(bad)};
            CHECK(moq_cmaf_packager_write(p, &s, 1, &out) == MOQ_ERR_PROTO);
            uint8_t crc[] = {0xff, 0xf0, 0x4c, 0x80, 0x01, 0x9f, 0xfc,
                             0, 0, 0x11, 0x22, 0x33};
            s.data = (moq_bytes_t){crc, sizeof(crc)};
            CHECK(moq_cmaf_packager_write(p, &s, 1, &out) == MOQ_OK);
            mdat = box(out, "mdat");
            CHECK(mdat.len == 3);
            if (mdat.len == 3) CHECK(memcmp(mdat.data, adts + 7, 3) == 0);
            s.dts = s.pts = 2048;
            crc[6] |= 1;
            CHECK(moq_cmaf_packager_write(p, &s, 1, &out) == MOQ_ERR_UNSUPPORTED);
            s.data = (moq_bytes_t){adts, sizeof(adts)};
            CHECK(moq_cmaf_packager_write(p, &s, 1, &out) == MOQ_OK);
            CHECK(sequence(out) == 3);
            s.dts = s.pts = 3072;
            s.data = (moq_bytes_t){adts, sizeof(adts) - 1};
            CHECK(moq_cmaf_packager_write(p, &s, 1, &out) == MOQ_ERR_PROTO);
        }
        moq_cmaf_packager_destroy(p);
    }
}
static void nal_boundaries(void)
{
    const unsigned widths[] = {1, 2, 4};
    for (size_t i = 0; i < sizeof(widths) / sizeof(widths[0]); i++) {
        unsigned width = widths[i];
        uint8_t record[sizeof(avcc)]; memcpy(record, avcc, sizeof(record));
        record[4] = (uint8_t)(0xfc | (width - 1));
        moq_cmaf_packager_cfg_t c = config(0);
        c.codec_config = (moq_bytes_t){record, sizeof(record)};
        moq_cmaf_packager_t *p = NULL;
        CHECK(moq_cmaf_packager_create(NULL, &c, &p) == MOQ_OK);
        if (!p) continue;
        size_t len = width == 1 ? 255 : width == 2 ? 65535 : 70000;
        uint8_t *data = malloc(len + 5);
        CHECK(data != NULL);
        if (!data) { moq_cmaf_packager_destroy(p); continue; }
        memset(data, 0x65, len + 5);
        data[0] = data[1] = data[2] = 0; data[3] = 1;
        moq_cmaf_packager_sample_t s = sample(0);
        s.data = (moq_bytes_t){data, len + 4};
        moq_bytes_t out;
        CHECK(moq_cmaf_packager_write(p, &s, 1, &out) == MOQ_OK);
        moq_bytes_t mdat = box(out, "mdat");
        CHECK(mdat.len == len + width);
        if (mdat.len == len + width) {
            size_t encoded = 0;
            for (unsigned k = 0; k < width; k++) encoded = (encoded << 8) | mdat.data[k];
            CHECK(encoded == len);
        }
        s.dts = s.pts = 3000;
        if (width < 4) {
            s.data.len++;
            CHECK(moq_cmaf_packager_write(p, &s, 1, &out) == MOQ_ERR_UNSUPPORTED);
        }
        s.data = (moq_bytes_t){nal, sizeof(nal)};
        CHECK(moq_cmaf_packager_write(p, &s, 1, &out) == MOQ_OK);
        CHECK(sequence(out) == 2);
        free(data);
        moq_cmaf_packager_destroy(p);
    }
    moq_cmaf_packager_cfg_t c = config(0);
    c.sample_format = MOQ_CMAF_SAMPLE_NAL_LENGTH_PREFIXED;
    moq_cmaf_packager_t *p = NULL;
    CHECK(moq_cmaf_packager_create(NULL, &c, &p) == MOQ_OK);
    if (!p) return;
    const uint8_t good[] = {0, 0, 0, 2, 0x65, 0x11};
    const uint8_t bad[] = {0, 0, 0, 3, 0x65, 0x11};
    const uint8_t sets[] = {0, 0, 0, 2, 0x67, 0x11};
    moq_cmaf_packager_sample_t s = sample(0);
    moq_bytes_t out;
    s.data = (moq_bytes_t){bad, sizeof(bad)};
    CHECK(moq_cmaf_packager_write(p, &s, 1, &out) == MOQ_ERR_PROTO);
    s.data = (moq_bytes_t){sets, sizeof(sets)};
    CHECK(moq_cmaf_packager_write(p, &s, 1, &out) == MOQ_ERR_PROTO);
    s.data = (moq_bytes_t){good, sizeof(good)};
    CHECK(moq_cmaf_packager_write(p, &s, 1, &out) == MOQ_OK);
    CHECK(sequence(out) == 1);
    moq_cmaf_packager_destroy(p);
}
typedef struct { size_t live, calls, fail; } allocator_t;
static void *allocate(size_t n, void *ctx)
{
    allocator_t *a = ctx;
    if (++a->calls == a->fail) return NULL;
    void *p = malloc(n);
    if (p) a->live += n;
    return p;
}
static void release(void *p, size_t n, void *ctx)
{
    allocator_t *a = ctx;
    if (p) { CHECK(a->live >= n); a->live -= n; }
    free(p);
}
static moq_cmaf_packager_t *local_allocator(allocator_t *a, moq_cmaf_packager_cfg_t *c)
{
    moq_alloc_t vt = {.ctx = a, .alloc = allocate, .free = release};
    moq_cmaf_packager_t *p = NULL;
    moq_result_t rc = moq_cmaf_packager_create(&vt, c, &p);
    CHECK(rc == MOQ_OK || rc == MOQ_ERR_NOMEM);
    memset(&vt, 0, sizeof(vt)); /* instance must have copied its value */
    return p;
}
static void allocation_failures(void)
{
    for (size_t fail = 1; fail <= 8; fail++) {
        allocator_t a = {0, 0, fail};
        moq_cmaf_packager_cfg_t c = config(0);
        moq_cmaf_packager_t *p = local_allocator(&a, &c);
        if (p) moq_cmaf_packager_destroy(p);
        CHECK(a.live == 0);
    }
    for (size_t fail = 1; fail <= 8; fail++) {
        allocator_t a = {0, 0, 0};
        moq_cmaf_packager_cfg_t c = config(0); c.rebase_timestamps = true;
        moq_cmaf_packager_t *p = local_allocator(&a, &c);
        if (!p) continue;
        a.fail = a.calls + fail;
        moq_cmaf_packager_sample_t samples[17];
        for (size_t i = 0; i < 17; i++) samples[i] = sample((int64_t)i * 3000);
        moq_bytes_t out;
        moq_result_t rc = moq_cmaf_packager_write(p, samples, 17, &out);
        CHECK(rc == MOQ_OK || rc == MOQ_ERR_NOMEM);
        if (rc == MOQ_ERR_NOMEM) {
            CHECK(out.data == NULL && out.len == 0);
            a.fail = 0;
            moq_cmaf_packager_sample_t retry = sample(900000);
            CHECK(moq_cmaf_packager_write(p, &retry, 1, &out) == MOQ_OK);
            CHECK(sequence(out) == 1 && decode_time(out) == 0);
        }
        a.fail = 0;
        moq_cmaf_packager_destroy(p);
        CHECK(a.live == 0);
    }
    for (size_t fail = 1; fail <= 12; fail++) {
        allocator_t a = {0, 0, 0};
        moq_cmaf_packager_cfg_t c = config(0);
        moq_cmaf_packager_t *p = local_allocator(&a, &c);
        if (!p) continue;
        moq_cmaf_packager_sample_t first = sample(0);
        moq_bytes_t out;
        CHECK(moq_cmaf_packager_write(p, &first, 1, &out) == MOQ_OK);
        uint8_t big[2048]; memset(big, 0x65, sizeof(big));
        big[0] = big[1] = 0; big[2] = 1;
        moq_cmaf_packager_sample_t samples[17];
        for (size_t i = 0; i < 17; i++) {
            samples[i] = sample(3000 + (int64_t)i * 3000);
            samples[i].data = (moq_bytes_t){big, sizeof(big)};
        }
        a.fail = a.calls + fail;
        moq_result_t rc = moq_cmaf_packager_write(p, samples, 17, &out);
        CHECK(rc == MOQ_OK || rc == MOQ_ERR_NOMEM);
        if (rc == MOQ_ERR_NOMEM) {
            CHECK(out.data == NULL && out.len == 0);
            a.fail = 0;
            first = sample(3000);
            CHECK(moq_cmaf_packager_write(p, &first, 1, &out) == MOQ_OK);
            CHECK(sequence(out) == 2 && decode_time(out) == 3000);
        }
        a.fail = 0;
        moq_cmaf_packager_destroy(p);
        CHECK(a.live == 0);
    }
}
static void abi_and_config(void)
{
    union { moq_cmaf_packager_cfg_t aligned; uint8_t bytes[sizeof(moq_cmaf_packager_cfg_t) + 16]; } u;
    for (size_t n = 0; n <= sizeof(u.bytes); n++) {
        memset(&u, 0xa5, sizeof(u));
        moq_cmaf_packager_cfg_init_sized(&u.aligned, n);
        size_t written = n < 4 ? 0 : n < sizeof(u.aligned) ? n : sizeof(u.aligned);
        if (written) CHECK(u.aligned.struct_size == written);
        for (size_t i = written; i < sizeof(u.bytes); i++) CHECK(u.bytes[i] == 0xa5);
    }
    moq_cmaf_packager_cfg_init_sized(NULL, SIZE_MAX);
    memset(&u, 0xa5, sizeof(u));
    moq_cmaf_packager_cfg_init_sized(&u.aligned, SIZE_MAX);
    CHECK(u.aligned.struct_size == sizeof(u.aligned));
    for (size_t i = sizeof(u.aligned); i < sizeof(u.bytes); i++) CHECK(u.bytes[i] == 0xa5);
    moq_cmaf_packager_cfg_t c = config(0);
    c.struct_size = (uint32_t)MOQ_CMAF_PACKAGER_CFG_V0_SIZE;
    moq_cmaf_packager_t *p = NULL;
    CHECK(moq_cmaf_packager_create(NULL, &c, &p) == MOQ_OK);
    moq_cmaf_packager_destroy(p);
    c.struct_size = UINT32_MAX;
    CHECK(moq_cmaf_packager_create(NULL, &c, &p) == MOQ_OK);
    moq_cmaf_packager_destroy(p);
    c.struct_size = (uint32_t)MOQ_CMAF_PACKAGER_CFG_V0_SIZE;
    c.struct_size--;
    p = (moq_cmaf_packager_t *)&c;
    CHECK(moq_cmaf_packager_create(NULL, &c, &p) == MOQ_ERR_INVAL && p == NULL);
    p = (moq_cmaf_packager_t *)&c;
    CHECK(moq_cmaf_packager_create(NULL, NULL, &p) == MOQ_ERR_INVAL && p == NULL);
    for (size_t i = 0; i < sizeof(codecs) / sizeof(codecs[0]); i++) {
        c = config(i);
        uint8_t invalid = 0;
        c.codec_config = (moq_bytes_t){&invalid, 1};
        CHECK(moq_cmaf_packager_create(NULL, &c, &p) == MOQ_ERR_PROTO && p == NULL);
    }
    c = config(0); c.sample_entry = MOQ_CMAF_ENTRY_AVC3;
    CHECK(moq_cmaf_packager_create(NULL, &c, &p) == MOQ_ERR_UNSUPPORTED);
    c.sample_entry = MOQ_CMAF_ENTRY_HEV1;
    CHECK(moq_cmaf_packager_create(NULL, &c, &p) == MOQ_ERR_UNSUPPORTED);
    c = config(2); c.timescale = 90000;
    CHECK(moq_cmaf_packager_create(NULL, &c, &p) == MOQ_ERR_INVAL);
    c = config(4); c.samplerate = 44100;
    CHECK(moq_cmaf_packager_create(NULL, &c, &p) == MOQ_ERR_INVAL);
}
int main(void)
{
    five_codecs(); timing_and_aliases(); aac_formats(); nal_boundaries();
    allocation_failures(); abi_and_config();
    printf("packager feature failures=%d\n", failures);
    return failures ? 1 : 0;
}
