/*
 * CENC in LOCMAF (sections 10.3, 13, 15.8, 18): the senc fields in the
 * decoder, the rebuilt senc, saiz and saio, the IV size override, and every
 * CENC refusal with its reason.
 */
#include "locmaf_fixtures.h"
#include "media_builders.h"   /* protected CMAF Headers */

#include <moq/vi64.h>

#define FULL  MOQ_LOCMAF_ELEMENT_FULL_HEADER
#define DELTA MOQ_LOCMAF_ELEMENT_DELTA_HEADER

#define IV8  "0102030405060708"
#define IV16 "0102030405060708090A0B0C0D0E0F10"

/* type, properties_length, the properties given in hex, payload_len bytes. */
static size_t object(uint8_t *out, size_t cap, unsigned kind, const char *props_hex,
                     size_t payload_len)
{
    uint8_t props[2048];
    size_t pn = lm_hex(props_hex, props, sizeof(props));
    size_t n = moq_vi64_encode(kind, out, cap);
    n += moq_vi64_encode(pn, out + n, cap - n);
    if (pn + payload_len > cap - n) { fprintf(stderr, "object overflow\n"); exit(2); }
    memcpy(out + n, props, pn);
    n += pn;
    lm_fill(out + n, payload_len, 0x3C);
    return n + payload_len;
}

static moq_result_t push(lm_dec_t *dec, uint64_t o, unsigned kind, const char *props,
                         size_t payload, moq_locmaf_chunk_t *ch, moq_locmaf_reason_t *why)
{
    /* The chunk borrows the Object's bytes (its IVs, the mdat payload), so
     * they stay until the next push. */
    static uint8_t obj[4096];
    size_t n = object(obj, sizeof(obj), kind, props, payload);
    *why = (moq_locmaf_reason_t)0xEEEE;
    memset(ch, 0, sizeof(*ch));
    return moq_locmaf_decoder_push(&dec->d, 0, o, (moq_bytes_t){ obj, n }, ch,
                                   sizeof(*ch), why);
}

static void expect_full(const moq_locmaf_track_t *t, const char *props, size_t payload,
                        moq_result_t rc, moq_locmaf_reason_t r)
{
    lm_dec_t dec;
    CHECK_RC(lm_dec_open(&dec, t, 0, 0, 0), MOQ_OK);
    moq_locmaf_chunk_t ch;
    moq_locmaf_reason_t why;
    moq_result_t got = push(&dec, 0, FULL, props, payload, &ch, &why);
    if (got != rc || why != r) fprintf(stderr, "  full header: %s (+%zu)\n", props, payload);
    CHECK_RC(got, rc);
    CHECK_WHY(why, r);
    lm_dec_close(&dec);
}

static uint32_t rd32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3];
}

/* The saio offset of a written chunk that starts with its moof. */
static uint32_t saio_offset(const uint8_t *chunk, size_t len)
{
    for (size_t i = 4; i + 16 <= len; i++)
        if (memcmp(chunk + i, "saio", 4) == 0) return rd32(chunk + i + 12);
    return 0;
}

static bool has_box(const uint8_t *chunk, size_t len, const char *type)
{
    for (size_t i = 4; i + 4 <= len; i++)
        if (memcmp(chunk + i, type, 4) == 0) return true;
    return false;
}

static void decode_fixture(size_t seq, moq_locmaf_chunk_t *ch, lm_dec_t *dec,
                           uint8_t *obj, uint8_t *out, size_t *out_len)
{
    const lm_fixture_t *f = &lm_sequences[seq].items[0];
    moq_locmaf_track_t t = lm_fixture_track(f);
    CHECK_RC(lm_dec_open(dec, &t, 0, 0, 0), MOQ_OK);
    size_t on = lm_fixture_object(f, obj, 512);
    moq_locmaf_reason_t why;
    memset(ch, 0, sizeof(*ch));
    CHECK_RC(moq_locmaf_decoder_push(&dec->d, 0, 0, (moq_bytes_t){ obj, on }, ch,
                                     sizeof(*ch), &why), MOQ_OK);
    CHECK_WHY(why, MOQ_LOCMAF_REASON_NONE);
    uint8_t want[512];
    size_t wn = lm_fixture_canonical(f, want, sizeof(want));
    *out_len = 0;
    CHECK_RC(moq_locmaf_write_cmaf(&t, ch, out, 512, out_len), MOQ_OK);
    CHECK_BYTES(out, *out_len, want, wn);
}

static void test_fixtures(void)
{
    uint8_t obj[512], out[512];
    size_t len;
    moq_locmaf_chunk_t ch;
    lm_dec_t dec;

    /* F9: cenc, IV 8, subsamples [1, 2] */
    decode_fixture(7, &ch, &dec, obj, out, &len);
    CHECK(ch.has_aux_info && ch.has_subsamples && ch.per_sample_iv_size == 8);
    CHECK_EQ(ch.ivs.len, 16);
    for (size_t i = 0; i < 16 && ch.ivs.len == 16 && ch.ivs.data; i++)
        CHECK(ch.ivs.data[i] == i + 1);
    CHECK(ch.sample_count == 2 && ch.samples);
    if (ch.sample_count == 2 && ch.samples) {
        CHECK(ch.samples[0].size == 100 && ch.samples[1].size == 60);
        CHECK(ch.samples[0].subsample_count == 1 && ch.samples[1].subsample_count == 2);
        CHECK(ch.samples[0].subsample_index == 0 && ch.samples[1].subsample_index == 1);
    }
    CHECK_EQ(ch.subsample_total, 3);
    if (ch.subsample_total == 3) {
        CHECK(ch.subsamples[0].clear_bytes == 10 && ch.subsamples[0].protected_bytes == 90);
        CHECK(ch.subsamples[1].clear_bytes == 5 && ch.subsamples[1].protected_bytes == 20);
        CHECK(ch.subsamples[2].clear_bytes == 5 && ch.subsamples[2].protected_bytes == 30);
    }
    /* saio points at the first IV, 151 bytes into the moof (section 15.8) */
    CHECK_EQ(saio_offset(out, len), 151);
    CHECK(len > 151 && out[151] == 0x01 && out[150] == 0x02 && out[149] == 0x00);
    lm_dec_close(&dec);

    /* F10: cbcs constant IV, one subsample: no IV, the map still rebuilt */
    decode_fixture(8, &ch, &dec, obj, out, &len);
    CHECK(ch.has_aux_info && ch.has_subsamples && ch.per_sample_iv_size == 0);
    CHECK(ch.ivs.len == 0 && ch.subsample_total == 1);
    CHECK_EQ(saio_offset(out, len), 0x91);
    CHECK(len > 0x92 && out[0x91] == 0x00 && out[0x92] == 0x01);   /* subsample_count 1 */
    lm_dec_close(&dec);

    /* F11: cbcs full-sample: protected, no aux info, no CENC boxes */
    decode_fixture(9, &ch, &dec, obj, out, &len);
    CHECK(!ch.has_aux_info && !ch.has_subsamples);
    CHECK(!has_box(out, len, "senc") && !has_box(out, len, "saiz") &&
          !has_box(out, len, "saio"));
    lm_dec_close(&dec);
}

static void test_deltas(void)
{
    moq_locmaf_track_t p8 = lm_track_p8();
    moq_locmaf_chunk_t ch;
    moq_locmaf_reason_t why;
    lm_dec_t dec;
    const char *f9 = "01 01 64 09 10 " IV8 "090A0B0C0D0E0F10 0A 00"
                     "0B 02 01 02 0D 03 0A 05 05 0E 02 0F 03 5A 14 1E";

    /* an inherited CENC delta: IVs resent, counts and entries inherited */
    CHECK_RC(lm_dec_open(&dec, &p8, 0, 0, 0), MOQ_OK);
    CHECK_RC(push(&dec, 0, FULL, f9, 160, &ch, &why), MOQ_OK);
    CHECK_RC(push(&dec, 1, DELTA, "09 10 A1A2A3A4A5A6A7A8 B1B2B3B4B5B6B7B8", 160, &ch, &why),
             MOQ_OK);
    CHECK(ch.has_aux_info && ch.has_subsamples && ch.ivs.len == 16);
    if (ch.ivs.len == 16 && ch.ivs.data) CHECK(ch.ivs.data[0] == 0xA1 && ch.ivs.data[8] == 0xB1);
    CHECK(ch.subsample_total == 3 && ch.subsamples && ch.subsamples[2].protected_bytes == 30);
    /* an empty delta keeps the IVs too */
    CHECK_RC(push(&dec, 2, DELTA, "", 160, &ch, &why), MOQ_OK);
    CHECK(ch.ivs.len == 16 && ch.ivs.data && ch.ivs.data[0] == 0xA1);
    /* IVs inherited at a new n no longer fit it */
    CHECK_RC(push(&dec, 3, DELTA, "0E 01", 100, &ch, &why), MOQ_ERR_PROTO);
    CHECK_WHY(why, MOQ_LOCMAF_REASON_IV_LENGTH);
    lm_dec_close(&dec);

    /* field 16 overrides the tenc IV size, 16 to 8 and back */
    {
        moq_locmaf_track_t p16 = lm_track_p16();
        CHECK_RC(lm_dec_open(&dec, &p16, 0, 0, 0), MOQ_OK);
        CHECK_RC(push(&dec, 0, FULL, "09 08 " IV8 " 0A 00 0E 01 10 08", 4, &ch, &why), MOQ_OK);
        CHECK(ch.per_sample_iv_size == 8 && ch.ivs.len == 8 && ch.has_aux_info);
        CHECK_RC(push(&dec, 1, DELTA, "09 10 " IV16 " 10 10", 4, &ch, &why), MOQ_OK);
        CHECK(ch.per_sample_iv_size == 16 && ch.ivs.len == 16);
        /* deleting the override falls back to tenc: 16 still */
        CHECK_RC(push(&dec, 2, DELTA, "1B 01 10", 4, &ch, &why), MOQ_OK);
        CHECK(ch.per_sample_iv_size == 16);
        /* changing the size without the IVs */
        /* ID 16 is absent again: 0 + 8, with IVs still of 16 bytes */
        CHECK_RC(push(&dec, 3, DELTA, "10 10", 4, &ch, &why), MOQ_ERR_PROTO);
        CHECK_WHY(why, MOQ_LOCMAF_REASON_IV_LENGTH);
        lm_dec_close(&dec);
        /* sizes a track cannot have */
        expect_full(&p16, "09 00 0A 00 0E 01 10 04", 4, MOQ_ERR_PROTO,
                    MOQ_LOCMAF_REASON_VALUE_RANGE);
        expect_full(&p16, "09 00 0A 00 0E 01 10 11", 4, MOQ_ERR_PROTO,
                    MOQ_LOCMAF_REASON_VALUE_RANGE);
    }
}

static void test_refusals(void)
{
    moq_locmaf_track_t ta = lm_track_a(), p8 = lm_track_p8(), p16 = lm_track_p16();
    moq_locmaf_track_t p0 = lm_track_p0();
    const moq_result_t P = MOQ_ERR_PROTO;

    /* section 10.3: every CENC field on a clear track */
    expect_full(&ta, "09 00 0A 00 0E 01", 4, P, MOQ_LOCMAF_REASON_CLEAR_TRACK_CENC);
    expect_full(&ta, "0A 00 0B 01 00 0E 01", 4, P, MOQ_LOCMAF_REASON_CLEAR_TRACK_CENC);
    expect_full(&ta, "0A 00 0D 00 0E 01", 4, P, MOQ_LOCMAF_REASON_CLEAR_TRACK_CENC);
    expect_full(&ta, "0A 00 0E 01 0F 00", 4, P, MOQ_LOCMAF_REASON_CLEAR_TRACK_CENC);
    expect_full(&ta, "0A 00 0E 01 10 00", 4, P, MOQ_LOCMAF_REASON_CLEAR_TRACK_CENC);
    /* IV bytes not n x IV size; a map with no IVs on a per-sample-IV track */
    expect_full(&p8, "09 07 01020304050607 0A 00 0E 01", 4, P, MOQ_LOCMAF_REASON_IV_LENGTH);
    expect_full(&p8, "09 09 010203040506070809 0A 00 0E 01", 4, P,
                MOQ_LOCMAF_REASON_IV_LENGTH);
    expect_full(&p8, "0A 00 0B 01 01 0D 01 02 0E 01 0F 01 02", 4, P,
                MOQ_LOCMAF_REASON_IV_LENGTH);
    /* constant-IV tracks carry ID 9 empty, or not at all */
    expect_full(&p0, "09 00 0A 00 0B 01 01 0D 01 02 0E 01 0F 01 02", 4, MOQ_OK,
                MOQ_LOCMAF_REASON_NONE);
    expect_full(&p0, "09 08 " IV8 " 0A 00 0E 01", 4, P, MOQ_LOCMAF_REASON_IV_LENGTH);
    /* entry lists without counts; counts without entry lists; aux at n = 0 */
    expect_full(&p8, "09 08 " IV8 " 0A 00 0D 01 02 0E 01", 4, P,
                MOQ_LOCMAF_REASON_CENC_LAYOUT);
    expect_full(&p8, "09 08 " IV8 " 0A 00 0E 01 0F 01 04", 4, P,
                MOQ_LOCMAF_REASON_CENC_LAYOUT);
    expect_full(&p8, "09 08 " IV8 " 0A 00 0B 01 01 0E 01", 4, P,
                MOQ_LOCMAF_REASON_CENC_LAYOUT);
    expect_full(&p8, "09 00 0A 00 0B 00 0E 00", 0, P, MOQ_LOCMAF_REASON_CENC_LAYOUT);
    expect_full(&p8, "09 00 0A 00 0E 00", 0, P, MOQ_LOCMAF_REASON_CENC_LAYOUT);
    /* a sample with no subsamples is unconstrained */
    expect_full(&p8, "09 08 " IV8 " 0A 00 0B 01 00 0D 00 0E 01 0F 00", 4, MOQ_OK,
                MOQ_LOCMAF_REASON_NONE);
    /* section 18: clear + protected = the sample's size */
    expect_full(&p8, "09 08 " IV8 " 0A 00 0B 01 01 0D 01 01 0E 01 0F 01 02", 4, P,
                MOQ_LOCMAF_REASON_SUBSAMPLE_SUM);
    /* section 18: field widths (clear 16 bits, protected 32, counts 16) */
    expect_full(&p8, "09 08 " IV8 " 0A 00 0B 01 01 0D 03 C1 00 00 0E 01 0F 01 00", 4, P,
                MOQ_LOCMAF_REASON_VALUE_RANGE);
    expect_full(&p8, "09 08 " IV8 " 0A 00 0B 01 01 0D 01 00 0E 01 0F 05 F1 00 00 00 00", 4,
                P, MOQ_LOCMAF_REASON_VALUE_RANGE);
    expect_full(&p8, "09 08 " IV8 " 0A 00 0B 03 C1 00 00 0E 01", 4, P,
                MOQ_LOCMAF_REASON_VALUE_RANGE);
    /* list lengths, n = 2 */
    expect_full(&p8, "01 01 02 09 10 " IV8 IV8 " 0A 00 0B 01 01 0E 02", 4, P,
                MOQ_LOCMAF_REASON_LIST_LENGTH);
    expect_full(&p8, "01 01 02 09 10 " IV8 IV8 " 0A 00 0B 02 01 01 0D 01 01 0E 02"
                     " 0F 02 01 01", 4, P, MOQ_LOCMAF_REASON_LIST_LENGTH);
    expect_full(&p8, "01 01 02 09 10 " IV8 IV8 " 0A 00 0B 02 01 01 0D 02 01 01 0E 02"
                     " 0F 03 01 01 01", 4, P, MOQ_LOCMAF_REASON_LIST_LENGTH);
    expect_full(&p8, "01 01 02 09 10 " IV8 IV8 " 0A 00 0B 02 01 01 0D 02 01 01 0E 02"
                     " 0F 02 01 01", 4, MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    /* section 18: auxiliary information within 8 bits: IV 16, 41 subsamples
     * is 16 + 2 + 246 = 264 */
    {
        char props[1024];
        size_t k = 0;
        k += (size_t)snprintf(props + k, sizeof(props) - k, "09 10 " IV16 " 0A 00 0B 01 29 0D 29");
        for (int i = 0; i < 41; i++) k += (size_t)snprintf(props + k, sizeof(props) - k, " 01");
        k += (size_t)snprintf(props + k, sizeof(props) - k, " 0E 01 0F 29");
        for (int i = 0; i < 41; i++) k += (size_t)snprintf(props + k, sizeof(props) - k, " 00");
        expect_full(&p16, props, 41, P, MOQ_LOCMAF_REASON_AUX_INFO_SIZE);
        /* 39 subsamples: 16 + 2 + 234 = 252 fits */
        k = 0;
        k += (size_t)snprintf(props + k, sizeof(props) - k, "09 10 " IV16 " 0A 00 0B 01 27 0D 27");
        for (int i = 0; i < 39; i++) k += (size_t)snprintf(props + k, sizeof(props) - k, " 01");
        k += (size_t)snprintf(props + k, sizeof(props) - k, " 0E 01 0F 27");
        for (int i = 0; i < 39; i++) k += (size_t)snprintf(props + k, sizeof(props) - k, " 00");
        expect_full(&p16, props, 39, MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    }
    /* section 18: the subsample total bounded before the entry lists */
    {
        lm_dec_t dec;
        CHECK_RC(lm_dec_open(&dec, &p8, 0, 2, 0), MOQ_OK);
        moq_locmaf_chunk_t ch;
        moq_locmaf_reason_t why;
        CHECK_RC(push(&dec, 0, FULL, "09 08 " IV8 " 0A 00 0B 01 03 0D 03 01 01 01 0E 01"
                                    " 0F 03 00 00 01", 4, &ch, &why), MOQ_ERR_LIMIT);
        CHECK_WHY(why, MOQ_LOCMAF_REASON_LIMIT);
        CHECK_RC(push(&dec, 1, FULL, "09 08 " IV8 " 0A 00 0B 01 02 0D 02 01 01 0E 01"
                                    " 0F 02 01 01", 4, &ch, &why), MOQ_OK);
        lm_dec_close(&dec);
    }
}

/* Sections 13.1 and 15.8: on a protected track whose effective per-sample
 * IV size is above zero, every sample's IV travels in field 9; a chunk
 * without them cannot rebuild its senc. Size 0 (a constant IV) needs none. */
static void test_missing_ivs(void)
{
    moq_locmaf_track_t p8 = lm_track_p8(), p16 = lm_track_p16(), p0 = lm_track_p0();
    const moq_result_t P = MOQ_ERR_PROTO;
    const moq_locmaf_reason_t R = MOQ_LOCMAF_REASON_IV_LENGTH;
    expect_full(&p8, "0A 00 0E 01", 4, P, R);
    expect_full(&p16, "0A 00 0E 01", 4, P, R);
    expect_full(&p8, "01 01 02 0A 00 0E 02", 4, P, R);
    expect_full(&p16, "06 02 0A 00 0E 02", 4, P, R);
    /* an override to a nonzero size needs them too */
    expect_full(&p8, "0A 00 0E 01 10 10", 4, P, R);
    /* legal: a constant-IV track, no samples, an override to size 0 */
    expect_full(&p0, "0A 00 0E 01", 4, MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    expect_full(&p0, "01 01 02 0A 00 0E 02", 4, MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    expect_full(&p8, "0A 00 0E 00", 0, MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    expect_full(&p16, "0A 00 0E 01 10 00", 4, MOQ_OK, MOQ_LOCMAF_REASON_NONE);

    /* a delta deleting field 9 leaves the samples without IVs */
    static const struct { moq_locmaf_track_t (*track)(void); const char *full; } rows[] = {
        { lm_track_p8, "09 08 " IV8 " 0A 00 0E 01" },
        { lm_track_p16, "09 10 " IV16 " 0A 00 0E 01" },
    };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        moq_locmaf_track_t t = rows[i].track();
        lm_dec_t dec;
        CHECK_RC(lm_dec_open(&dec, &t, 0, 0, 0), MOQ_OK);
        moq_locmaf_chunk_t ch;
        moq_locmaf_reason_t why;
        CHECK_RC(push(&dec, 0, FULL, rows[i].full, 4, &ch, &why), MOQ_OK);
        CHECK(ch.has_aux_info && ch.ivs.len == t.default_per_sample_iv_size);
        CHECK_RC(push(&dec, 1, DELTA, "1B 01 09", 4, &ch, &why), MOQ_ERR_PROTO);
        CHECK_WHY(why, R);
        lm_dec_close(&dec);
    }
}

/* The same rule on tracks taken from CMAF Headers: the scheme in schm does
 * not enter it, the IV size in tenc does. Each row decodes one sample with
 * no field 9 and writes its chunk with and without the auxiliary
 * information. */
static void test_missing_ivs_by_scheme(void)
{
    static const uint8_t avcc[] = { 0x01, 0x64, 0x00, 0x1E, 0xFF };
    static const uint8_t kid[16] = { 0x4B, 0x49, 0x44 };
    static const uint8_t civ[16] = { 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8,
                                     0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xD0 };
    static const struct {
        const char *scheme;
        uint8_t tenc_version, pattern, iv;
        const char *with_ivs;
    } rows[] = {
        { "cenc", 0, 0x00, 8, "09 08 " IV8 " 0A 00 0E 01" },
        { "cenc", 0, 0x00, 16, "09 10 " IV16 " 0A 00 0E 01" },
        { "cbcs", 1, 0x19, 16, "09 10 " IV16 " 0A 00 0E 01" },
        { "cbcs", 1, 0x19, 0, "0A 00 0E 01" },               /* constant IV */
    };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        moq_test_cenc_init_opts_t o;
        memset(&o, 0, sizeof(o));
        o.pattern = rows[i].pattern;
        o.with_trex = true;
        o.trex[0] = 1;
        o.trex[1] = 1024;
        o.trex[3] = 0x02000000u;
        if (rows[i].iv == 0) {
            o.constant_iv_size = sizeof(civ);
            o.constant_iv = civ;
            o.constant_iv_len = sizeof(civ);
        }
        uint8_t raw[1024];
        size_t len = moq_test_build_cmaf_init_cenc_ex(raw, 1, 90000, 64, 64, avcc, sizeof(avcc),
                                                      rows[i].scheme, 1, rows[i].iv, kid,
                                                      rows[i].tenc_version, MOQ_TEST_CENC_OK, &o);
        moq_cmaf_init_info_t init;
        CHECK_RC(moq_cmaf_parse_init_sized((moq_bytes_t){ raw, len }, &init, sizeof(init)),
                 MOQ_OK);
        moq_locmaf_track_t t;
        memset(&t, 0, sizeof(t));
        CHECK_RC(moq_locmaf_track_from_cmaf(&init, &t, sizeof(t)), MOQ_OK);
        CHECK(t.is_protected && t.default_per_sample_iv_size == rows[i].iv);
        bool needs = rows[i].iv > 0;

        expect_full(&t, "0A 00 0E 01", 4, needs ? MOQ_ERR_PROTO : MOQ_OK,
                    needs ? MOQ_LOCMAF_REASON_IV_LENGTH : MOQ_LOCMAF_REASON_NONE);

        lm_dec_t dec;
        CHECK_RC(lm_dec_open(&dec, &t, 0, 0, 0), MOQ_OK);
        moq_locmaf_chunk_t ch;
        moq_locmaf_reason_t why;
        CHECK_RC(push(&dec, 0, FULL, rows[i].with_ivs, 4, &ch, &why), MOQ_OK);
        CHECK(ch.has_aux_info == needs && ch.ivs.len == rows[i].iv);
        uint8_t out[1024];
        size_t olen = 0;
        CHECK_RC(moq_locmaf_write_cmaf(&t, &ch, out, sizeof(out), &olen), MOQ_OK);
        ch.has_aux_info = false;
        ch.ivs = (moq_bytes_t){ NULL, 0 };
        olen = 0x5555;
        moq_result_t rc = moq_locmaf_write_cmaf(&t, &ch, out, sizeof(out), &olen);
        if (rc != (needs ? MOQ_ERR_INVAL : MOQ_OK))
            fprintf(stderr, "  %.4s, IV size %u: written without aux info\n", rows[i].scheme,
                    rows[i].iv);
        CHECK_RC(rc, needs ? MOQ_ERR_INVAL : MOQ_OK);
        lm_dec_close(&dec);
    }
}

int main(void)
{
    test_fixtures();
    test_deltas();
    test_refusals();
    test_missing_ivs();
    test_missing_ivs_by_scheme();
    return lm_finish("locmaf_cenc");
}
