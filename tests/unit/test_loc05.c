/* LOC-05 Codec String (draft-ietf-moq-loc-05 2.3.1.3) through the LOC codec.
 * Every byte string here is an independent spec vector (draft-22 8.1 vi64,
 * 8.3 Key-Value-Pair framing, 10.7 Immutable Properties), not output of the
 * encoder under test. */
#include <moq/loc.h>
#include "test_support.h"
#include "test_oom_support.h"
#include <stdlib.h>
#include <string.h>

#define D16 MOQ_VERSION_DRAFT_16
#define D18 MOQ_VERSION_DRAFT_18
#define D22 MOQ_VERSION_DRAFT_22
#define P01 MOQ_LOC_PROFILE_01
#define P04 MOQ_LOC_PROFILE_04
#define P05 MOQ_LOC_PROFILE_05

static moq_bytes_t B(const uint8_t *data, size_t len)
{
    return (moq_bytes_t){data, len};
}

/* Parse under LOC05 with both outputs; returns the result. */
static moq_result_t parse05(moq_bytes_t in, moq_loc_metadata_t *m, moq_loc_ext_t *e)
{
    moq_loc_metadata_init_sized(m, sizeof(*m));
    moq_loc_ext_init_sized(e, sizeof(*e));
    return moq_loc_parse_metadata_ex(D22, P05, in, m, sizeof(*m), e, sizeof(*e));
}

/* The span a successful parse reports must be the input bytes themselves. */
static int codec_at(const moq_loc_ext_t *e, const uint8_t *in, size_t offset, size_t len)
{
    int failures = 0;
    MOQ_TEST_CHECK(e->present == MOQ_LOC_EXT_HAS_CODEC_STRING);
    MOQ_TEST_CHECK(e->codec_string.data == in + offset);
    MOQ_TEST_CHECK_EQ_SIZE(e->codec_string.len, len);
    return failures;
}

static int valid_vectors(void)
{
    int failures = 0;
    moq_loc_metadata_t m;
    moq_loc_ext_t e;

    static const uint8_t v1[] = {0x11, 0x04, 'o', 'p', 'u', 's'};
    MOQ_TEST_CHECK_EQ_INT(parse05(B(v1, sizeof(v1)), &m, &e), MOQ_OK);
    failures += codec_at(&e, v1, 2, 4);
    MOQ_TEST_CHECK(m.present == 0);
    MOQ_TEST_CHECK_EQ_SIZE(e.struct_size, sizeof(e));

    static const uint8_t v2[] = {0x11, 0x0b, 'a', 'v', 'c', '1', '.', '6', '4',
                                 '0', '0', '1', 'f'};
    MOQ_TEST_CHECK_EQ_INT(parse05(B(v2, sizeof(v2)), &m, &e), MOQ_OK);
    failures += codec_at(&e, v2, 2, 11);

    /* Timescale 48000 (vi64 c0 bb 80), Timestamp 0, Codec String "opus". */
    static const uint8_t v3[] = {0x08, 0xc0, 0xbb, 0x80, 0x08, 0x00,
                                 0x01, 0x04, 'o', 'p', 'u', 's'};
    MOQ_TEST_CHECK_EQ_INT(parse05(B(v3, sizeof(v3)), &m, &e), MOQ_OK);
    MOQ_TEST_CHECK(m.present == (MOQ_LOC_METADATA_HAS_TIMESCALE |
                                 MOQ_LOC_METADATA_HAS_TIMESTAMP));
    MOQ_TEST_CHECK_EQ_U64(m.timescale, 48000);
    MOQ_TEST_CHECK_EQ_U64(m.timestamp, 0);
    failures += codec_at(&e, v3, 8, 4);

    /* After Audio Config 0x0F: delta 2. */
    static const uint8_t v4[] = {0x0f, 0x01, 0xaa, 0x02, 0x04, 'o', 'p', 'u', 's'};
    MOQ_TEST_CHECK_EQ_INT(parse05(B(v4, sizeof(v4)), &m, &e), MOQ_OK);
    MOQ_TEST_CHECK(m.present == MOQ_LOC_METADATA_HAS_AUDIO_CONFIG);
    failures += codec_at(&e, v4, 5, 4);

    /* Inside Immutable Properties: the inner delta restarts at 0, and the
     * span is the inner offset of the original bytes. */
    static const uint8_t v5[] = {0x0b, 0x06, 0x11, 0x04, 'o', 'p', 'u', 's'};
    MOQ_TEST_CHECK_EQ_INT(parse05(B(v5, sizeof(v5)), &m, &e), MOQ_OK);
    failures += codec_at(&e, v5, 4, 4);

    /* Non-minimal vi64 Length and Delta Type are valid (8.1). */
    static const uint8_t v6[] = {0x11, 0x80, 0x04, 'o', 'p', 'u', 's'};
    MOQ_TEST_CHECK_EQ_INT(parse05(B(v6, sizeof(v6)), &m, &e), MOQ_OK);
    failures += codec_at(&e, v6, 3, 4);
    static const uint8_t v7[] = {0x80, 0x11, 0x04, 'o', 'p', 'u', 's'};
    MOQ_TEST_CHECK_EQ_INT(parse05(B(v7, sizeof(v7)), &m, &e), MOQ_OK);
    failures += codec_at(&e, v7, 3, 4);

    /* Multi-byte UTF-8, kept exactly; leading/trailing whitespace is kept;
     * U+000B is not ASCII whitespace; any identifier is accepted. */
    static const struct { uint8_t bytes[8]; size_t len; } ok[] = {
        {{0x11, 0x02, 0xc3, 0xa9}, 4},
        {{0x11, 0x03, 0xef, 0xbf, 0xbd}, 5},
        {{0x11, 0x04, 0xf4, 0x8f, 0xbf, 0xbf}, 6},
        {{0x11, 0x05, ' ', 'o', 'p', 'u', 's'}, 7},
        {{0x11, 0x05, 'o', 'p', 'u', 's', '\t'}, 7},
        {{0x11, 0x05, 0x0b, 'o', 'p', 'u', 's'}, 7},
        {{0x11, 0x04, 'x', 'y', 'z', '9'}, 6},
    };
    for (size_t i = 0; i < sizeof(ok) / sizeof(ok[0]); i++) {
        MOQ_TEST_CHECK_EQ_INT(parse05(B(ok[i].bytes, ok[i].len), &m, &e), MOQ_OK);
        failures += codec_at(&e, ok[i].bytes, 2, ok[i].len - 2);
    }
    return failures;
}

/* The Length field crosses vi64 widths at 127/128 and 16383/16384 (D22 8.1
 * Table 3); 65535 is the 8.3 cap and 65536 is past it. */
static int length_boundaries(void)
{
    int failures = 0;
    static const struct { size_t len; uint8_t header[4]; size_t header_len; } rows[] = {
        {127,   {0x11, 0x7f}, 2},
        {128,   {0x11, 0x80, 0x80}, 3},
        {16383, {0x11, 0xbf, 0xff}, 3},
        {16384, {0x11, 0xc0, 0x40, 0x00}, 4},
        {65535, {0x11, 0xc0, 0xff, 0xff}, 4},
    };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        size_t total = rows[i].header_len + rows[i].len;
        uint8_t *in = (uint8_t *)malloc(total);
        MOQ_TEST_CHECK(in != NULL);
        if (!in) continue;
        memcpy(in, rows[i].header, rows[i].header_len);
        memset(in + rows[i].header_len, 'a', rows[i].len);
        moq_loc_metadata_t m;
        moq_loc_ext_t e;
        MOQ_TEST_CHECK_EQ_INT(parse05(B(in, total), &m, &e), MOQ_OK);
        failures += codec_at(&e, in, rows[i].header_len, rows[i].len);

        /* The encoder's minimal header is the same literal. */
        moq_loc_metadata_t none;
        moq_loc_metadata_init_sized(&none, sizeof(none));
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK_EQ_INT(moq_loc_encode_metadata_ex(moq_alloc_default(), D22, P05,
            &none, &e, &out), MOQ_OK);
        if (out) {
            MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(out), total);
            if (moq_rcbuf_len(out) == total)
                MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(out), in, total) == 0);
        }
        moq_rcbuf_decref(out);
        free(in);
    }
    /* 65536 is refused by framing on every revision. */
    static const uint8_t over[] = {0x11, 0xc1, 0x00, 0x00};
    moq_loc_metadata_t m;
    moq_loc_ext_t e;
    MOQ_TEST_CHECK_EQ_INT(parse05(B(over, sizeof(over)), &m, &e), MOQ_ERR_PROTO);
    MOQ_TEST_CHECK_EQ_INT(moq_loc_parse_metadata(D22, P04, B(over, sizeof(over)),
        &m, sizeof(m)), MOQ_ERR_PROTO);
    /* A 64-byte value: one Length byte on D22, two on D16 (where 0x11 is an
     * unknown LOC-01 property and is skipped). */
    uint8_t d22[2 + 64], d16[3 + 64];
    d22[0] = 0x11; d22[1] = 0x40; memset(d22 + 2, 'a', 64);
    d16[0] = 0x11; d16[1] = 0x40; d16[2] = 0x40; memset(d16 + 3, 'a', 64);
    MOQ_TEST_CHECK_EQ_INT(parse05(B(d22, sizeof(d22)), &m, &e), MOQ_OK);
    failures += codec_at(&e, d22, 2, 64);
    MOQ_TEST_CHECK_EQ_INT(moq_loc_parse_metadata(D16, P01, B(d16, sizeof(d16)),
        &m, sizeof(m)), MOQ_OK);
    MOQ_TEST_CHECK(m.present == 0);
    return failures;
}

static int refusals(void)
{
    int failures = 0;
    static const struct { uint8_t bytes[8]; size_t len; moq_result_t expect; } bad[] = {
        {{0x11, 0x00}, 2, MOQ_ERR_PROTO},                         /* empty */
        {{0x11, 0x01, ' '}, 3, MOQ_ERR_PROTO},                    /* space only */
        {{0x11, 0x03, 0x09, 0x0a, 0x0d}, 5, MOQ_ERR_PROTO},       /* whitespace only */
        {{0x11, 0x01, 0x0c}, 3, MOQ_ERR_PROTO},                   /* form feed only */
        {{0x11, 0x05, 'o', 'p', 0x00, 'u', 's'}, 7, MOQ_ERR_PROTO}, /* embedded NUL */
        {{0x11, 0x01, 0x00}, 3, MOQ_ERR_PROTO},                   /* NUL only */
        {{0x11, 0x02, 0xc0, 0xaf}, 4, MOQ_ERR_PROTO},             /* overlong */
        {{0x11, 0x03, 0xe0, 0x80, 0xaf}, 5, MOQ_ERR_PROTO},       /* overlong */
        {{0x11, 0x04, 0xf0, 0x80, 0x80, 0xaf}, 6, MOQ_ERR_PROTO}, /* overlong */
        {{0x11, 0x03, 0xed, 0xa0, 0x80}, 5, MOQ_ERR_PROTO},       /* surrogate */
        {{0x11, 0x04, 0xf4, 0x90, 0x80, 0x80}, 6, MOQ_ERR_PROTO}, /* > U+10FFFF */
        {{0x11, 0x02, 0xe2, 0x82}, 4, MOQ_ERR_PROTO},             /* truncated */
        {{0x11, 0x01, 0x80}, 3, MOQ_ERR_PROTO},                   /* continuation */
        {{0x11, 0x01, 0xff}, 3, MOQ_ERR_PROTO},
        {{0x11, 0x02, 0xc1, 0xbf}, 4, MOQ_ERR_PROTO},
        {{0x11, 0x05, 'o', 'p', 'u', 's'}, 6, MOQ_ERR_PROTO},     /* short value */
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        moq_bytes_t in = B(bad[i].bytes, bad[i].len);
        /* Both outputs stay untouched on any failure. */
        moq_loc_metadata_t m;
        moq_loc_ext_t e;
        memset(&m, 0xa5, sizeof(m));
        memset(&e, 0x5a, sizeof(e));
        moq_loc_metadata_t m0 = m;
        moq_loc_ext_t e0 = e;
        MOQ_TEST_CHECK_EQ_INT(moq_loc_parse_metadata_ex(D22, P05, in, &m, sizeof(m),
            &e, sizeof(e)), bad[i].expect);
        MOQ_TEST_CHECK(memcmp(&m, &m0, sizeof(m)) == 0);
        MOQ_TEST_CHECK(memcmp(&e, &e0, sizeof(e)) == 0);
        /* Omitting the extension output does not omit validation. */
        MOQ_TEST_CHECK_EQ_INT(moq_loc_parse_metadata(D22, P05, in, &m, sizeof(m)),
                              bad[i].expect);
        MOQ_TEST_CHECK_EQ_INT(moq_loc_parse_metadata_ex(D22, P05, in, &m, sizeof(m),
            NULL, 0), bad[i].expect);
        /* LOC-04 still skips 0x11 as unknown, unless the framing itself is
         * broken (the short value). */
        moq_loc_ext_t e4;
        moq_loc_ext_init_sized(&e4, sizeof(e4));
        moq_result_t r4 = moq_loc_parse_metadata_ex(D22, P04, in, &m, sizeof(m),
                                                    &e4, sizeof(e4));
        MOQ_TEST_CHECK_EQ_INT(r4, i == sizeof(bad) / sizeof(bad[0]) - 1
                                       ? MOQ_ERR_PROTO : MOQ_OK);
        if (r4 == MOQ_OK) MOQ_TEST_CHECK(e4.present == 0 && m.present == 0);
    }
    /* A second Codec String anywhere in the block, equal or not. */
    static const uint8_t dup_mutable[] = {0x11, 0x04, 'o', 'p', 'u', 's',
                                          0x00, 0x04, 'o', 'p', 'u', 's'};
    static const uint8_t dup_both[] = {0x0b, 0x06, 0x11, 0x04, 'o', 'p', 'u', 's',
                                       0x06, 0x04, 'o', 'p', 'u', 's'};
    static const uint8_t dup_inner[] = {0x0b, 0x0c, 0x11, 0x04, 'o', 'p', 'u', 's',
                                        0x00, 0x04, 'o', 'p', 'u', 's'};
    const moq_bytes_t dups[] = {B(dup_mutable, sizeof(dup_mutable)),
                                B(dup_both, sizeof(dup_both)),
                                B(dup_inner, sizeof(dup_inner))};
    for (size_t i = 0; i < 3; i++) {
        moq_loc_metadata_t m;
        moq_loc_ext_t e;
        MOQ_TEST_CHECK_EQ_INT(parse05(dups[i], &m, &e), MOQ_ERR_UNSUPPORTED);
        MOQ_TEST_CHECK_EQ_INT(moq_loc_parse_metadata(D22, P05, dups[i], &m, sizeof(m)),
                              MOQ_ERR_UNSUPPORTED);
        MOQ_TEST_CHECK_EQ_INT(moq_loc_parse_metadata(D22, P04, dups[i], &m, sizeof(m)),
                              MOQ_OK);
    }
    return failures;
}

static int arguments_and_sizes(void)
{
    int failures = 0;
    static const uint8_t v1[] = {0x11, 0x04, 'o', 'p', 'u', 's'};
    moq_loc_metadata_t m;
    moq_loc_ext_t e;
    /* LOC05 belongs to draft 22 only; the compatibility selector stays 04. */
    MOQ_TEST_CHECK_EQ_INT(moq_loc_parse_metadata_ex(D16, P05, B(v1, 6), &m, sizeof(m),
        &e, sizeof(e)), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(moq_loc_parse_metadata_ex(D18, P05, B(v1, 6), &m, sizeof(m),
        &e, sizeof(e)), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(moq_loc_profile_for_version(D22), P04);
    /* An extension output below its v0 floor, or overlapping out. */
    MOQ_TEST_CHECK_EQ_INT(moq_loc_parse_metadata_ex(D22, P05, B(v1, 6), &m, sizeof(m),
        &e, MOQ_LOC_EXT_V0_SIZE - 1), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(moq_loc_parse_metadata_ex(D22, P05, B(v1, 6), &m, sizeof(m),
        (moq_loc_ext_t *)(void *)((uint8_t *)&m + 8), sizeof(e)), MOQ_ERR_INVAL);
    /* A caller with a larger record gets this library's bytes, stamped, and
     * nothing past them. */
    struct { moq_loc_ext_t e; uint8_t tail[16]; } big;
    memset(&big, 0xee, sizeof(big));
    MOQ_TEST_CHECK_EQ_INT(moq_loc_parse_metadata_ex(D22, P05, B(v1, 6), &m, sizeof(m),
        &big.e, sizeof(big)), MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(big.e.struct_size, sizeof(moq_loc_ext_t));
    for (size_t i = 0; i < sizeof(big.tail); i++) MOQ_TEST_CHECK(big.tail[i] == 0xee);
    /* The initializer honours the caller's size. */
    memset(&big, 0xee, sizeof(big));
    moq_loc_ext_init_sized(&big.e, 4);
    MOQ_TEST_CHECK_EQ_SIZE(big.e.struct_size, 4);
    MOQ_TEST_CHECK(big.e.present == 0xeeeeeeeeu);
    moq_loc_ext_init_sized(&big.e, 3);   /* writes nothing */
    MOQ_TEST_CHECK_EQ_SIZE(big.e.struct_size, 4);
    return failures;
}

static int encoding(void)
{
    int failures = 0;
    oom_alloc_state_t state = {0};
    moq_alloc_t alloc = oom_allocator(&state);
    moq_loc_metadata_t m;
    moq_loc_metadata_init_sized(&m, sizeof(m));
    moq_loc_ext_t e;
    moq_loc_ext_init_sized(&e, sizeof(e));
    e.present = MOQ_LOC_EXT_HAS_CODEC_STRING;
    e.codec_string = B((const uint8_t *)"opus", 4);

    static const uint8_t e1[] = {0x11, 0x04, 'o', 'p', 'u', 's'};
    moq_rcbuf_t *out = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_loc_encode_metadata_ex(&alloc, D22, P05, &m, &e, &out), MOQ_OK);
    MOQ_TEST_CHECK(out && moq_rcbuf_len(out) == sizeof(e1) &&
                   memcmp(moq_rcbuf_data(out), e1, sizeof(e1)) == 0);
    moq_rcbuf_decref(out);

    /* The Codec String follows the other keys, timestamp 0x10 included. */
    static const uint8_t e2[] = {0x08, 0xc0, 0xbb, 0x80, 0x08, 0x00,
                                 0x01, 0x04, 'o', 'p', 'u', 's'};
    m.present = MOQ_LOC_METADATA_HAS_TIMESCALE | MOQ_LOC_METADATA_HAS_TIMESTAMP;
    m.timescale = 48000;
    m.timestamp = 0;
    out = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_loc_encode_metadata_ex(&alloc, D22, P05, &m, &e, &out), MOQ_OK);
    MOQ_TEST_CHECK(out && moq_rcbuf_len(out) == sizeof(e2) &&
                   memcmp(moq_rcbuf_data(out), e2, sizeof(e2)) == 0);
    moq_rcbuf_decref(out);

    /* Without a Codec String, LOC05 bytes are LOC04 bytes; an empty
     * extension record is the same as none. */
    moq_rcbuf_t *a = NULL, *b = NULL, *c = NULL;
    moq_loc_ext_t empty;
    moq_loc_ext_init_sized(&empty, sizeof(empty));
    MOQ_TEST_CHECK_EQ_INT(moq_loc_encode_metadata(&alloc, D22, P04, &m, &a), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(moq_loc_encode_metadata(&alloc, D22, P05, &m, &b), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(moq_loc_encode_metadata_ex(&alloc, D22, P04, &m, &empty, &c), MOQ_OK);
    MOQ_TEST_CHECK(a && b && c && moq_rcbuf_len(a) == moq_rcbuf_len(b) &&
                   moq_rcbuf_len(a) == moq_rcbuf_len(c) &&
                   memcmp(moq_rcbuf_data(a), moq_rcbuf_data(b), moq_rcbuf_len(a)) == 0 &&
                   memcmp(moq_rcbuf_data(a), moq_rcbuf_data(c), moq_rcbuf_len(a)) == 0);
    moq_rcbuf_decref(a);
    moq_rcbuf_decref(b);
    moq_rcbuf_decref(c);

    /* Every refusal happens before allocation and leaves the output alone. */
    moq_rcbuf_t *const sentinel = (moq_rcbuf_t *)(uintptr_t)0x1;
    static const struct { const char *bytes; size_t len; } invalid[] = {
        {"", 0}, {" \t", 2}, {"op\0us", 5}, {"\xed\xa0\x80", 3}, {"\xc0\xaf", 2},
    };
    moq_loc_metadata_init_sized(&m, sizeof(m));
    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        e.codec_string = B((const uint8_t *)invalid[i].bytes, invalid[i].len);
        uint64_t before = state.alloc_count;
        out = sentinel;
        MOQ_TEST_CHECK_EQ_INT(moq_loc_encode_metadata_ex(&alloc, D22, P05, &m, &e, &out),
                              MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(out == sentinel);
        MOQ_TEST_CHECK(state.alloc_count == before);
    }
    uint8_t *huge = (uint8_t *)malloc(65536);
    MOQ_TEST_CHECK(huge != NULL);
    if (huge) {
        memset(huge, 'a', 65536);
        e.codec_string = B(huge, 65536);
        out = sentinel;
        MOQ_TEST_CHECK_EQ_INT(moq_loc_encode_metadata_ex(&alloc, D22, P05, &m, &e, &out),
                              MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(out == sentinel);
        free(huge);
    }
    e.codec_string = B(NULL, 4);   /* incoherent span */
    MOQ_TEST_CHECK_EQ_INT(moq_loc_encode_metadata_ex(&alloc, D22, P05, &m, &e, &out),
                          MOQ_ERR_INVAL);
    /* A revision without the property never drops it. */
    e.codec_string = B((const uint8_t *)"opus", 4);
    uint64_t before = state.alloc_count;
    out = sentinel;
    MOQ_TEST_CHECK_EQ_INT(moq_loc_encode_metadata_ex(&alloc, D22, P04, &m, &e, &out),
                          MOQ_ERR_UNSUPPORTED);
    MOQ_TEST_CHECK_EQ_INT(moq_loc_encode_metadata_ex(&alloc, D16, P01, &m, &e, &out),
                          MOQ_ERR_UNSUPPORTED);
    MOQ_TEST_CHECK(out == sentinel && state.alloc_count == before);
    /* A malformed extension record. */
    e.present = 0x2;
    MOQ_TEST_CHECK_EQ_INT(moq_loc_encode_metadata_ex(&alloc, D22, P05, &m, &e, &out),
                          MOQ_ERR_INVAL);
    e.present = MOQ_LOC_EXT_HAS_CODEC_STRING;
    e.struct_size = MOQ_LOC_EXT_V0_SIZE - 1;
    MOQ_TEST_CHECK_EQ_INT(moq_loc_encode_metadata_ex(&alloc, D22, P05, &m, &e, &out),
                          MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(out == sentinel);
    MOQ_TEST_CHECK(state.balance == 0);

    /* Allocation failure publishes nothing and leaks nothing. */
    e.struct_size = (uint32_t)sizeof(e);
    state.fail_at = state.alloc_count + 1;
    out = sentinel;
    MOQ_TEST_CHECK_EQ_INT(moq_loc_encode_metadata_ex(&alloc, D22, P05, &m, &e, &out),
                          MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(out == sentinel && state.balance == 0);
    return failures;
}

/* Parse what the encoder wrote and get the exact bytes back, NUL-free
 * multi-byte text included. */
static int round_trip(void)
{
    int failures = 0;
    static const uint8_t text[] = {'h', 'e', 'v', '1', 0xc3, 0xa9, 0xf0, 0x9f, 0x8e, 0xa5};
    moq_loc_metadata_t m;
    moq_loc_metadata_init_sized(&m, sizeof(m));
    moq_loc_ext_t e;
    moq_loc_ext_init_sized(&e, sizeof(e));
    e.present = MOQ_LOC_EXT_HAS_CODEC_STRING;
    e.codec_string = B(text, sizeof(text));
    moq_rcbuf_t *out = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_loc_encode_metadata_ex(moq_alloc_default(), D22, P05, &m,
        &e, &out), MOQ_OK);
    if (!out) return failures + 1;
    moq_loc_ext_t back;
    MOQ_TEST_CHECK_EQ_INT(parse05(B(moq_rcbuf_data(out), moq_rcbuf_len(out)), &m, &back),
                          MOQ_OK);
    MOQ_TEST_CHECK(back.present == MOQ_LOC_EXT_HAS_CODEC_STRING &&
                   back.codec_string.len == sizeof(text) &&
                   memcmp(back.codec_string.data, text, sizeof(text)) == 0);
    moq_rcbuf_decref(out);
    return failures;
}

int main(void)
{
    static const struct { const char *name; int (*run)(void); } cases[] = {
        {"valid_vectors", valid_vectors},
        {"length_boundaries", length_boundaries},
        {"refusals", refusals},
        {"arguments_and_sizes", arguments_and_sizes},
        {"encoding", encoding},
        {"round_trip", round_trip},
    };
    int failures = 0;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        int n = cases[i].run();
        printf("CASE %s: %s (%d failed checks)\n", cases[i].name, n ? "FAIL" : "PASS", n);
        failures += n;
    }
    printf("%s: %zu cases, %d failures\n", failures ? "FAIL" : "PASS",
           sizeof(cases) / sizeof(cases[0]), failures);
    return failures != 0;
}
