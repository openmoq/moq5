/*
 * moq_locmaf_parse_object(): the element split of an Object (section 7.1),
 * header framing (7.2, 7.3), genBox carriage (8) and validation (18), and
 * rawBoxes (9.1, 9.2, 18). Every refusal pins its code and its reason.
 */
#include "locmaf_fixtures.h"

static moq_result_t parse(const uint8_t *p, size_t n, moq_locmaf_object_view_t *v,
                          moq_locmaf_gen_box_t *boxes, size_t cap, size_t *count,
                          moq_locmaf_reason_t *why)
{
    *why = (moq_locmaf_reason_t)0xEEEE;
    *count = 0xEEEE;
    return moq_locmaf_parse_object((moq_bytes_t){ p, n }, v, sizeof(*v),
                                   boxes, cap, count, why);
}

/* A refusal: the code, the reason, no count and an untouched view. */
static void expect_refused(const char *hex, moq_result_t rc, moq_locmaf_reason_t r)
{
    uint8_t buf[512];
    size_t n = lm_hex(hex, buf, sizeof(buf));
    moq_locmaf_object_view_t v;
    memset(&v, 0xA5, sizeof(v));
    moq_locmaf_gen_box_t boxes[4];
    memset(boxes, 0xA5, sizeof(boxes));
    size_t count;
    moq_locmaf_reason_t why;
    moq_result_t got = parse(buf, n, &v, boxes, 4, &count, &why);
    if (got != rc || why != r)
        fprintf(stderr, "  object: %s\n", hex);
    CHECK_RC(got, rc);
    CHECK_WHY(why, r);
    CHECK_EQ(count, 0);
    const uint8_t *raw = (const uint8_t *)&v;
    bool untouched = true;
    for (size_t i = 0; i < sizeof(v); i++) untouched &= raw[i] == 0xA5;
    CHECK(untouched);
}

static void expect_raw_refused(const uint8_t *boxes, size_t len, moq_result_t rc,
                               moq_locmaf_reason_t r)
{
    uint8_t obj[1024];
    obj[0] = 4;
    memcpy(obj + 1, boxes, len);
    moq_locmaf_object_view_t v;
    moq_locmaf_gen_box_t gb[2];
    size_t count;
    moq_locmaf_reason_t why;
    CHECK_RC(parse(obj, len + 1, &v, gb, 2, &count, &why), rc);
    CHECK_WHY(why, r);
    CHECK_EQ(count, 0);
}

static void expect_raw_ok(const uint8_t *boxes, size_t len)
{
    uint8_t obj[1024];
    obj[0] = 4;
    memcpy(obj + 1, boxes, len);
    moq_locmaf_object_view_t v;
    size_t count;
    moq_locmaf_reason_t why;
    CHECK_RC(parse(obj, len + 1, &v, NULL, 0, &count, &why), MOQ_OK);
    CHECK_WHY(why, MOQ_LOCMAF_REASON_NONE);
    CHECK_EQ(v.kind, MOQ_LOCMAF_OBJECT_RAW_BOXES);
    CHECK(v.raw_boxes.data == obj + 1 && v.raw_boxes.len == len);
    CHECK(v.properties.len == 0 && v.mdat_payload.len == 0 && count == 0);
}

static void test_fixture_shapes(void)
{
    for (size_t s = 0; s < lm_sequence_count; s++) {
        for (size_t i = 0; i < lm_sequences[s].count; i++) {
            const lm_fixture_t *f = &lm_sequences[s].items[i];
            uint8_t obj[512];
            size_t n = lm_fixture_object(f, obj, sizeof(obj));
            moq_locmaf_object_view_t v;
            memset(&v, 0, sizeof(v));
            moq_locmaf_gen_box_t boxes[4];
            size_t count;
            moq_locmaf_reason_t why;
            CHECK_RC(parse(obj, n, &v, boxes, 4, &count, &why), MOQ_OK);
            CHECK_WHY(why, MOQ_LOCMAF_REASON_NONE);
            CHECK_EQ(v.struct_size, MOQ_LOCMAF_OBJECT_VIEW_V0_SIZE);
            /* Each sequence starts with its full header; the rest are deltas. */
            CHECK_EQ(v.kind, f->object_id ? MOQ_LOCMAF_OBJECT_DELTA
                                          : MOQ_LOCMAF_OBJECT_FULL);
            CHECK_EQ(v.mdat_payload.len, f->payload_len);
            if (f->payload_len)
                CHECK(v.mdat_payload.data == obj + n - f->payload_len);
            CHECK_EQ(count, v.gen_box_count);
        }
    }
}

static void test_element_refusals(void)
{
    /* Section 7.1: an unknown leading element is malformed, never skipped. */
    expect_refused("00 00", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_UNKNOWN_ELEMENT);
    expect_refused("05 00", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_UNKNOWN_ELEMENT);
    expect_refused("40 40 00", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_UNKNOWN_ELEMENT);
    /* ... and after a genBox. */
    expect_refused("01 04 66726565 05 00", MOQ_ERR_PROTO,
                   MOQ_LOCMAF_REASON_UNKNOWN_ELEMENT);
    /* Sections 7.1, 9.2: rawBoxes only first. */
    expect_refused("01 04 66726565 04 00000008 66726565", MOQ_ERR_PROTO,
                   MOQ_LOCMAF_REASON_RAW_BOXES_NOT_FIRST);
    /* Exactly one header. */
    expect_refused("01 04 66726565", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_NO_HEADER);
    expect_refused("", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_EMPTY_OBJECT);
    /* vi64 running past the Object. */
    expect_refused("C0", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_TRUNCATED);
    expect_refused("02", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_TRUNCATED);
    expect_refused("01", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_TRUNCATED);
    expect_refused("01 04 66726565", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_NO_HEADER);
    expect_refused("01 04 66726565 C0", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_TRUNCATED);
    /* Section 7.2: the property block is exactly properties_length. */
    expect_refused("02 05 0A 00 0E 01", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_TRUNCATED);
    expect_refused("02 03 0A 00 0E 01", MOQ_ERR_PROTO,
                   MOQ_LOCMAF_REASON_PROPERTY_FRAMING);
    expect_refused("02 03 01 05 00 DEAD", MOQ_ERR_PROTO,
                   MOQ_LOCMAF_REASON_PROPERTY_FRAMING);
    expect_refused("02 01 0A", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_PROPERTY_FRAMING);
    /* Sections 8.1, 8.3, 18: box_size bounds. */
    expect_refused("01 00 02 00", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_GEN_BOX_SIZE);
    expect_refused("01 01 66 02 00", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_GEN_BOX_SIZE);
    expect_refused("01 02 6672 02 00", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_GEN_BOX_SIZE);
    expect_refused("01 03 667265 02 00", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_GEN_BOX_SIZE);
    expect_refused("01 F0 FF FF FF FC 66726565 02 00", MOQ_ERR_PROTO,
                   MOQ_LOCMAF_REASON_GEN_BOX_SIZE);
    /* 0xFFFFFFFB is a legal size: the bytes are simply not there. */
    expect_refused("01 F0 FF FF FF FB 66726565 02 00", MOQ_ERR_PROTO,
                   MOQ_LOCMAF_REASON_TRUNCATED);
    expect_refused("01 08 66726565 02 00", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_TRUNCATED);
    /* genBoxes that are never genBoxes. */
    expect_refused("01 04 6D6F6F66 02 00", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_GEN_BOX_TYPE);
    expect_refused("01 04 6D646174 02 00", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_GEN_BOX_TYPE);
    expect_refused("01 04 66747970 02 00", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_GEN_BOX_TYPE);
    expect_refused("01 04 6D6F6F76 02 00", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_GEN_BOX_TYPE);
    /* Section 2: at most one styp and one prft per chunk. */
    expect_refused("01 0C 73747970 636D6673 00000000"
                   "01 0C 73747970 636D6673 00000000 02 00",
                   MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_GEN_BOX_REPEAT);
    expect_refused("01 18 70726674 00000000 00000001 0102030405060708 00000010"
                   "01 18 70726674 00000000 00000001 0102030405060708 00000010 02 00",
                   MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_GEN_BOX_REPEAT);
}

static void expect_gen_box(const char *type, const char *payload_hex,
                           moq_result_t rc, moq_locmaf_reason_t r)
{
    uint8_t payload[256], obj[512];
    size_t pl = lm_hex(payload_hex, payload, sizeof(payload));
    size_t n = 0;
    obj[n++] = 1;
    obj[n++] = (uint8_t)(0x80 | ((4 + pl) >> 8));
    obj[n++] = (uint8_t)(4 + pl);
    memcpy(obj + n, type, 4); n += 4;
    memcpy(obj + n, payload, pl); n += pl;
    obj[n++] = 2; obj[n++] = 0;
    moq_locmaf_object_view_t v;
    moq_locmaf_gen_box_t boxes[2];
    memset(boxes, 0xA5, sizeof(boxes));
    size_t count;
    moq_locmaf_reason_t why;
    moq_result_t got = parse(obj, n, &v, boxes, 2, &count, &why);
    if (got != rc || why != r) fprintf(stderr, "  genBox %.4s %s\n", type, payload_hex);
    CHECK_RC(got, rc);
    CHECK_WHY(why, r);
    if (rc == MOQ_OK) {
        CHECK_EQ(count, 1);
        CHECK(boxes[0].payload.len == pl && boxes[0].payload.data == obj + 7);
        CHECK_EQ(boxes[0].type, LM_FOURCC_T(type[0], type[1], type[2], type[3]));
    } else {
        CHECK_EQ(count, 0);
    }
}

static void test_gen_box_validators(void)
{
    /* styp: major, minor, whole brands. */
    expect_gen_box("styp", "636D6673 000000", MOQ_ERR_PROTO,
                   MOQ_LOCMAF_REASON_GEN_BOX_STRUCTURE);
    expect_gen_box("styp", "636D6673 00000000 636D66", MOQ_ERR_PROTO,
                   MOQ_LOCMAF_REASON_GEN_BOX_STRUCTURE);
    expect_gen_box("styp", "636D6673 00000000 636D6663", MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    /* prft v0 20, v1 24, v2 refused. */
    expect_gen_box("prft", "00000000 00000001 0102030405060708 00000010", MOQ_OK,
                   MOQ_LOCMAF_REASON_NONE);
    expect_gen_box("prft", "01000000 00000001 0102030405060708 0000000000000010",
                   MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    expect_gen_box("prft", "02000000 00000001 0102030405060708 0000000000000010",
                   MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_GEN_BOX_STRUCTURE);
    expect_gen_box("prft", "00000000 00000001 0102030405060708 0000001000",
                   MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_GEN_BOX_STRUCTURE);
    /* emsg v0 and v1, and each with an unterminated string. */
    expect_gen_box("emsg", "00000000 75726E00 7600 00000001 00000002 00000003 00000004 6869",
                   MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    expect_gen_box("emsg", "00000000 75726E00 76", MOQ_ERR_PROTO,
                   MOQ_LOCMAF_REASON_GEN_BOX_STRUCTURE);
    expect_gen_box("emsg", "00000000 75726E00 7600 00000001 00000002 000000",
                   MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_GEN_BOX_STRUCTURE);
    expect_gen_box("emsg", "01000000 00017700 0000000000017700 FFFFFFFF 00000001"
                           "75726E3A7400 00 6869", MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    expect_gen_box("emsg", "01000000 00017700 0000000000017700 FFFFFFFF 00000001"
                           "75726E3A74", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_GEN_BOX_STRUCTURE);
    expect_gen_box("emsg", "02000000 00", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_GEN_BOX_STRUCTURE);
    /* sidx: reference_count entries of 12 bytes, exactly. */
    expect_gen_box("sidx", "00000000 00000001 00017700 00000000 00000000 0000 0001"
                           "000003E8 00000400 90000000", MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    expect_gen_box("sidx", "00000000 00000001 00017700 00000000 00000000 0000 0002"
                           "000003E8 00000400 90000000", MOQ_ERR_PROTO,
                   MOQ_LOCMAF_REASON_GEN_BOX_STRUCTURE);
    expect_gen_box("sidx", "01000000 00000001 00017700 0000000000000000 0000000000000000"
                           "0000 0000", MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    /* uuid: the 16-byte usertype at least. */
    expect_gen_box("uuid", "000102030405060708090A0B0C0D0E", MOQ_ERR_PROTO,
                   MOQ_LOCMAF_REASON_GEN_BOX_STRUCTURE);
    expect_gen_box("uuid", "000102030405060708090A0B0C0D0E0F", MOQ_OK,
                   MOQ_LOCMAF_REASON_NONE);
    /* free and skip: any contents. */
    expect_gen_box("free", "", MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    expect_gen_box("skip", "DEADBEEF00", MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    /* A type with no validator is refused, never carried unvalidated. */
    expect_gen_box("abcd", "0102", MOQ_ERR_UNSUPPORTED,
                   MOQ_LOCMAF_REASON_GEN_BOX_UNSUPPORTED);

    /* Two emsg are fine. */
    {
        uint8_t ok[256];
        size_t m = lm_hex("01 1C 656D7367 00000000 6100 6200 00000001 00000002 00000003 00000004"
                          "01 1C 656D7367 00000000 6100 6200 00000001 00000002 00000003 00000005"
                          "02 04 0A 00 0E 00", ok, sizeof(ok));
        moq_locmaf_object_view_t v;
        moq_locmaf_gen_box_t boxes[2];
        size_t count;
        moq_locmaf_reason_t why;
        CHECK_RC(parse(ok, m, &v, boxes, 2, &count, &why), MOQ_OK);
        CHECK_EQ(count, 2);
    }

    /* Boundary sizes: 4 is a box with an empty payload. */
    {
        uint8_t obj[16];
        size_t n = lm_hex("01 04 66726565 02 04 0A 00 0E 00", obj, sizeof(obj));
        moq_locmaf_object_view_t v;
        moq_locmaf_gen_box_t boxes[1];
        size_t count;
        moq_locmaf_reason_t why;
        CHECK_RC(parse(obj, n, &v, boxes, 1, &count, &why), MOQ_OK);
        CHECK(count == 1 && boxes[0].payload.len == 0);
    }
}

static void test_uuid_and_buffer(void)
{
    const lm_fixture_t *f7 = &lm_sequences[5].items[0];
    CHECK(strcmp(f7->name, "F7") == 0);
    uint8_t obj[256];
    size_t n = lm_fixture_object(f7, obj, sizeof(obj));

    /* The uuid payload is kept intact: usertype first, then its data. */
    {
        moq_locmaf_object_view_t v;
        moq_locmaf_gen_box_t boxes[2];
        memset(boxes, 0, sizeof(boxes));
        size_t count;
        moq_locmaf_reason_t why;
        CHECK_RC(parse(obj, n, &v, boxes, 2, &count, &why), MOQ_OK);
        CHECK_EQ(count, 2);
        CHECK_EQ(boxes[0].type, LM_FOURCC_T('s','t','y','p'));
        CHECK_EQ(boxes[1].type, LM_FOURCC_T('u','u','i','d'));
        CHECK_EQ(boxes[1].payload.len, 17);
        if (boxes[1].payload.len == 17 && boxes[1].payload.data) {
            for (int i = 0; i < 16; i++) CHECK(boxes[1].payload.data[i] == i);
            CHECK(boxes[1].payload.data[16] == 0x78);
        }
        CHECK(boxes[0]._reserved == 0 && boxes[1]._reserved == 0);
    }

    /* BUFFER: the view is filled, the count reported, cap entries written. */
    {
        moq_locmaf_object_view_t v;
        size_t count;
        moq_locmaf_reason_t why;
        CHECK_RC(parse(obj, n, &v, NULL, 0, &count, &why), MOQ_ERR_BUFFER);
        CHECK_EQ(count, 2);
        CHECK(v.kind == MOQ_LOCMAF_OBJECT_FULL && v.gen_box_count == 2);
        CHECK(v.mdat_payload.len == 4);
        moq_locmaf_gen_box_t one[2];
        memset(one, 0xA5, sizeof(one));
        CHECK_RC(parse(obj, n, &v, one, 1, &count, &why), MOQ_ERR_BUFFER);
        CHECK_EQ(count, 2);
        CHECK_EQ(one[0].type, LM_FOURCC_T('s','t','y','p'));
        const uint8_t *raw = (const uint8_t *)&one[1];
        bool untouched = true;
        for (size_t i = 0; i < sizeof(one[1]); i++) untouched &= raw[i] == 0xA5;
        CHECK(untouched);
    }

    /* Malformed never surfaces a count, even past a valid genBox. */
    {
        uint8_t bad[256];
        size_t m = lm_hex("01 14 73747970 636D6673 00000000 636D6673 636D6663"
                          "01 04 61626364 02 00", bad, sizeof(bad));
        moq_locmaf_object_view_t v;
        memset(&v, 0xA5, sizeof(v));
        moq_locmaf_gen_box_t boxes[2];
        size_t count;
        moq_locmaf_reason_t why;
        CHECK_RC(parse(bad, m, &v, boxes, 2, &count, &why), MOQ_ERR_UNSUPPORTED);
        CHECK_EQ(count, 0);
        CHECK(v.struct_size == 0xA5A5A5A5u);
    }

    /* Arguments. */
    {
        moq_locmaf_object_view_t v;
        size_t count = 7;
        moq_locmaf_reason_t why;
        moq_locmaf_gen_box_t boxes[1];
        CHECK_RC(moq_locmaf_parse_object((moq_bytes_t){ obj, n }, NULL, sizeof(v),
                                         boxes, 1, &count, &why), MOQ_ERR_INVAL);
        CHECK_RC(moq_locmaf_parse_object((moq_bytes_t){ obj, n }, &v, sizeof(v),
                                         boxes, 1, NULL, &why), MOQ_ERR_INVAL);
        CHECK_RC(moq_locmaf_parse_object((moq_bytes_t){ obj, n }, &v,
                                         MOQ_LOCMAF_OBJECT_VIEW_V0_SIZE - 1,
                                         boxes, 1, &count, &why), MOQ_ERR_INVAL);
        CHECK_RC(moq_locmaf_parse_object((moq_bytes_t){ NULL, 1 }, &v, sizeof(v),
                                         boxes, 1, &count, &why), MOQ_ERR_INVAL);
        CHECK_RC(moq_locmaf_parse_object((moq_bytes_t){ obj, n }, &v, sizeof(v),
                                         NULL, 1, &count, &why), MOQ_ERR_INVAL);
        CHECK_EQ(count, 7);
        /* reason may be NULL. */
        CHECK_RC(moq_locmaf_parse_object((moq_bytes_t){ obj, n }, &v, sizeof(v),
                                         boxes, 0, &count, NULL), MOQ_ERR_BUFFER);
    }
}

static void test_vi64_non_minimal(void)
{
    /* Every vi64 in its longest form: type, length, IDs and values. */
    uint8_t obj[64];
    size_t n = lm_hex("FF 0000000000000002 FF 000000000000001D"
                      "FF 000000000000000A FF 0000000000000000"
                      "FF 000000000000000E 80 01 DEADBEEF", obj, sizeof(obj));
    moq_locmaf_object_view_t v;
    size_t count;
    moq_locmaf_reason_t why;
    CHECK_RC(parse(obj, n, &v, NULL, 0, &count, &why), MOQ_OK);
    CHECK_EQ(v.kind, MOQ_LOCMAF_OBJECT_FULL);
    CHECK_EQ(v.properties.len, 0x1D);
    CHECK_EQ(v.mdat_payload.len, 4);
}

static size_t base_chunk(uint8_t *out, size_t cap, const uint8_t *traf_extra,
                         size_t traf_len, const uint8_t *moof_extra, size_t moof_len)
{
    static const uint8_t payload[4] = { 1, 2, 3, 4 };
    lm_chunk_spec_t s;
    memset(&s, 0, sizeof(s));
    s.track_id = 1;
    s.tfdt_version = 1;
    s.tfhd_flags = 0x020010;
    s.tfhd_size = 4;
    s.trun_flags = 0x001;
    s.n = 1;
    s.payload = payload;
    s.payload_len = 4;
    s.traf_extra = traf_extra;
    s.traf_extra_len = traf_len;
    s.moof_extra = moof_extra;
    s.moof_extra_len = moof_len;
    return lm_build_chunk(&s, out, cap);
}

static void test_raw_boxes(void)
{
    uint8_t chunk[512];
    size_t n;

    /* One CMAF chunk, with each allowed traf child. */
    n = base_chunk(chunk, sizeof(chunk), NULL, 0, NULL, 0);
    expect_raw_ok(chunk, n);
    {
        uint8_t b[64];
        size_t k = lm_hex("0000001C 73626770 00000000 726F6C6C 00000001 00000001 00000001",
                          b, sizeof(b));                                   /* sbgp */
        n = base_chunk(chunk, sizeof(chunk), b, k, NULL, 0);
        expect_raw_ok(chunk, n);
        k = lm_hex("0000001A 73677064 01000000 726F6C6C 00000002 00000001 FFFF",
                   b, sizeof(b));                                          /* sgpd v1 */
        n = base_chunk(chunk, sizeof(chunk), b, k, NULL, 0);
        expect_raw_ok(chunk, n);
        k = lm_hex("00000030 73677064 02000000 73656967 00000014 00000001 00000001"
                   "00 00 01 08 000102030405060708090A0B0C0D0E0F", b, sizeof(b)); /* sgpd v2 seig */
        n = base_chunk(chunk, sizeof(chunk), b, k, NULL, 0);
        expect_raw_ok(chunk, n);
        k = lm_hex("0000001E 73756273 00000000 00000001 00000001 0001 0004 00 00 00000000",
                   b, sizeof(b));                                          /* subs */
        n = base_chunk(chunk, sizeof(chunk), b, k, NULL, 0);
        expect_raw_ok(chunk, n);
        k = lm_hex("00000018 75756964 000102030405060708090A0B0C0D0E0F", b, sizeof(b));
        n = base_chunk(chunk, sizeof(chunk), b, k, NULL, 0);               /* uuid */
        expect_raw_ok(chunk, n);
        k = lm_hex("00000022 70737368 00000000 1077EFECC0B24D02ACE33C1E52E2FB4B"
                   "00000002 ABCD", b, sizeof(b));                          /* pssh in moof */
        n = base_chunk(chunk, sizeof(chunk), NULL, 0, b, k);
        expect_raw_ok(chunk, n);
        /* A senc fits any IV size a track can have here (no track facts). */
        k = lm_hex("00000018 73656E63 00000000 00000001 0102030405060708", b, sizeof(b));
        n = base_chunk(chunk, sizeof(chunk), b, k, NULL, 0);
        expect_raw_ok(chunk, n);
    }

    /* Negatives. */
    {
        uint8_t b[64];
        size_t k;
        /* tfhd with an unknown flag bit */
        lm_chunk_spec_t s;
        memset(&s, 0, sizeof(s));
        static const uint8_t pl[4] = { 1, 2, 3, 4 };
        s.track_id = 1; s.tfdt_version = 1; s.tfhd_flags = 0x020040;
        s.trun_flags = 0x001; s.n = 1; s.payload = pl; s.payload_len = 4;
        n = lm_build_chunk(&s, chunk, sizeof(chunk));
        expect_raw_refused(chunk, n, MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_RAW_BOXES_MALFORMED);
        /* a trun whose sample table overruns: count 2, one record */
        static const uint32_t sizes[1] = { 4 };
        s.tfhd_flags = 0x020000; s.trun_flags = 0x201; s.sizes = sizes;
        n = lm_build_chunk(&s, chunk, sizeof(chunk));
        for (size_t i = 0; i + 8 <= n; i++) {
            if (memcmp(chunk + i, "trun", 4) == 0) { chunk[i + 11] = 2; break; }
        }
        expect_raw_refused(chunk, n, MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_RAW_BOXES_MALFORMED);
        /* a senc short by one byte for every IV size */
        k = lm_hex("00000017 73656E63 00000000 00000001 01020304050607", b, sizeof(b));
        n = base_chunk(chunk, sizeof(chunk), b, k, NULL, 0);
        expect_raw_refused(chunk, n, MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_RAW_BOXES_MALFORMED);
        /* sgpd v2 of type roll */
        k = lm_hex("0000001E 73677064 02000000 726F6C6C 00000002 00000001 00000001 FFFF",
                   b, sizeof(b));
        n = base_chunk(chunk, sizeof(chunk), b, k, NULL, 0);
        expect_raw_refused(chunk, n, MOQ_ERR_UNSUPPORTED,
                           MOQ_LOCMAF_REASON_RAW_BOXES_UNSUPPORTED);
        /* a meta box in traf */
        k = lm_hex("0000000C 6D657461 00000000", b, sizeof(b));
        n = base_chunk(chunk, sizeof(chunk), b, k, NULL, 0);
        expect_raw_refused(chunk, n, MOQ_ERR_UNSUPPORTED,
                           MOQ_LOCMAF_REASON_RAW_BOXES_UNSUPPORTED);
        /* a nested size-1 box */
        k = lm_hex("00000001 75756964 0000000000000018 000102030405060708090A0B0C0D0E0F",
                   b, sizeof(b));
        n = base_chunk(chunk, sizeof(chunk), b, k, NULL, 0);
        expect_raw_refused(chunk, n, MOQ_ERR_UNSUPPORTED,
                           MOQ_LOCMAF_REASON_RAW_BOXES_UNSUPPORTED);
        /* ftyp + moov (an in-band header) */
        k = lm_hex("00000010 66747970 636D6663 00000000 00000008 6D6F6F76", b, sizeof(b));
        expect_raw_refused(b, k, MOQ_ERR_UNSUPPORTED, MOQ_LOCMAF_REASON_RAW_BOXES_UNSUPPORTED);
        /* two chunks */
        size_t one = base_chunk(chunk, sizeof(chunk), NULL, 0, NULL, 0);
        memcpy(chunk + one, chunk, one);
        expect_raw_refused(chunk, 2 * one, MOQ_ERR_UNSUPPORTED,
                           MOQ_LOCMAF_REASON_RAW_BOXES_UNSUPPORTED);
        /* boxes that are not a chunk */
        k = lm_hex("00000008 66726565", b, sizeof(b));
        expect_raw_refused(b, k, MOQ_ERR_UNSUPPORTED, MOQ_LOCMAF_REASON_RAW_BOXES_UNSUPPORTED);
        /* a second styp before the moof */
        static const uint8_t styp2[] = {
            0, 0, 0, 0x10, 's', 't', 'y', 'p', 'c', 'm', 'f', 's', 0, 0, 0, 0,
            0, 0, 0, 0x10, 's', 't', 'y', 'p', 'c', 'm', 'f', 's', 0, 0, 0, 0 };
        memcpy(b, styp2, sizeof(styp2));
        n = base_chunk(chunk, sizeof(chunk), NULL, 0, NULL, 0);
        uint8_t joined[600];
        memcpy(joined, b, sizeof(styp2));
        memcpy(joined + sizeof(styp2), chunk, n);
        expect_raw_refused(joined, sizeof(styp2) + n, MOQ_ERR_PROTO,
                           MOQ_LOCMAF_REASON_RAW_BOXES_MALFORMED);
    }

    /* Section 9.1: complete boxes, no escapes, non-empty. */
    {
        uint8_t b[32];
        size_t k;
        expect_refused("04", MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_RAW_BOXES_MALFORMED);
        expect_refused("04 00000007 66726565", MOQ_ERR_PROTO,
                       MOQ_LOCMAF_REASON_RAW_BOXES_MALFORMED);
        expect_refused("04 00000000 66726565", MOQ_ERR_PROTO,
                       MOQ_LOCMAF_REASON_RAW_BOXES_MALFORMED);
        expect_refused("04 00000001 66726565 0000000000000010", MOQ_ERR_PROTO,
                       MOQ_LOCMAF_REASON_RAW_BOXES_MALFORMED);
        expect_refused("04 00000008 66726565 0000", MOQ_ERR_PROTO,
                       MOQ_LOCMAF_REASON_RAW_BOXES_MALFORMED);
        k = lm_hex("00000009 66726565", b, sizeof(b));
        expect_raw_refused(b, k, MOQ_ERR_PROTO, MOQ_LOCMAF_REASON_RAW_BOXES_MALFORMED);
    }
}

static void test_reason_names(void)
{
    CHECK(strcmp(moq_locmaf_reason_name(MOQ_LOCMAF_REASON_NONE), "none") == 0);
    CHECK(strcmp(moq_locmaf_reason_name(MOQ_LOCMAF_REASON_GEN_BOX_UNSUPPORTED),
                 "gen_box_unsupported") == 0);
    CHECK(strcmp(moq_locmaf_reason_name(MOQ_LOCMAF_REASON_SIZE_ESCAPE), "size_escape") == 0);
    CHECK(strcmp(moq_locmaf_reason_name(30u), "") == 0);
    CHECK(strcmp(moq_locmaf_reason_name(0xFFFFFFFFu), "") == 0);
    /* Every defined value has a distinct non-empty name. */
    const char *seen[64];
    size_t k = 0;
    for (uint32_t r = 1; r < 64; r++) {
        const char *nm = moq_locmaf_reason_name(r);
        if (!*nm) continue;
        for (size_t i = 0; i < k; i++) CHECK(strcmp(seen[i], nm) != 0);
        seen[k++] = nm;
    }
    CHECK_EQ(k, 29 + 11);
}

int main(void)
{
    test_fixture_shapes();
    test_element_refusals();
    test_gen_box_validators();
    test_uuid_and_buffer();
    test_vi64_non_minimal();
    test_raw_boxes();
    test_reason_names();
    return lm_finish("locmaf_wire");
}
