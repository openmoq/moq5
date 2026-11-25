/*
 * moq_locmaf_encoder_*(): CMAF chunks to the golden Objects (canonical
 * encoding, section 15.9), the full-header triggers, two-phase commit and
 * retry, rawBoxes, every chunk-reader refusal with its code and reason, and
 * the encoder counters.
 */
#include "locmaf_fixtures.h"

typedef struct enc_out {
    uint8_t buf[4096];
    size_t  len;
} enc_out_t;

static moq_result_t enc(lm_enc_t *e, uint64_t g, uint64_t o, const uint8_t *chunk,
                        size_t len, enc_out_t *out, moq_locmaf_reason_t *why)
{
    out->len = 0;
    *why = (moq_locmaf_reason_t)0xEEEE;
    return moq_locmaf_encode_chunk(&e->e, g, o, (moq_bytes_t){ chunk, len }, out->buf,
                                   sizeof(out->buf), &out->len, why);
}

static moq_locmaf_encoder_stats_t estats(const lm_enc_t *e)
{
    moq_locmaf_encoder_stats_t s;
    memset(&s, 0, sizeof(s));
    CHECK_RC(moq_locmaf_encoder_get_stats(&e->e, &s, sizeof(s)), MOQ_OK);
    return s;
}

/* The fixtures' canonical chunks encode to the fixture Objects, in order. */
static void test_fixture_objects(void)
{
    for (size_t s = 0; s < lm_sequence_count; s++) {
        const lm_sequence_t *seq = &lm_sequences[s];
        moq_locmaf_track_t track = lm_fixture_track(&seq->items[0]);
        lm_enc_t e;
        CHECK_RC(lm_enc_open(&e, &track, 0, 0, 0), MOQ_OK);
        for (size_t i = 0; i < seq->count; i++) {
            const lm_fixture_t *f = &seq->items[i];
            uint8_t chunk[512], want[512];
            size_t cl = lm_fixture_canonical(f, chunk, sizeof(chunk));
            size_t wl = lm_fixture_object(f, want, sizeof(want));
            size_t bound = 0;
            moq_locmaf_reason_t why;
            CHECK_RC(moq_locmaf_encoder_check(&e.e, (moq_bytes_t){ chunk, cl }, &bound, &why),
                     MOQ_OK);
            enc_out_t out;
            moq_result_t rc = enc(&e, 0, f->object_id, chunk, cl, &out, &why);
            if (rc != MOQ_OK) fprintf(stderr, "  fixture %s: %s\n", f->name,
                                      moq_locmaf_reason_name(why));
            CHECK_RC(rc, MOQ_OK);
            if (rc == MOQ_OK) {
                CHECK_BYTES(out.buf, out.len, want, wl);
                CHECK(out.len <= bound);
            }
            moq_locmaf_encoder_commit(&e.e);
        }
        lm_enc_close(&e);
    }
}

/* F3 written the way a muxer might: every tfhd default, sdi equal to trex,
 * mfhd sequence 7, tfdt version 0. */
static size_t f3a_noncanonical(uint8_t *out, size_t cap, uint8_t *payload)
{
    lm_fill(payload, 5, 0x30);
    lm_chunk_spec_t s;
    memset(&s, 0, sizeof(s));
    s.track_id = 1;
    s.mfhd_seq = 7;
    s.tfdt_version = 0;
    s.bmdt = 900000;
    s.tfhd_flags = 0x02003A;
    s.tfhd_sdi = 1;
    s.tfhd_dur = 3000;
    s.tfhd_size = 5;
    s.tfhd_flags_value = 0x02000000;
    s.trun_flags = 0x001;
    s.n = 1;
    s.payload = payload;
    s.payload_len = 5;
    return lm_build_chunk(&s, out, cap);
}

static void test_noncanonical_sources(void)
{
    moq_locmaf_track_t tv = lm_track_v();
    uint8_t chunk[512], payload[8], want[512];
    size_t cl = f3a_noncanonical(chunk, sizeof(chunk), payload);
    size_t wl = lm_fixture_object(&lm_sequences[1].items[0], want, sizeof(want));
    lm_enc_t e;
    CHECK_RC(lm_enc_open(&e, &tv, 0, 0, 0), MOQ_OK);
    enc_out_t out;
    moq_locmaf_reason_t why;
    CHECK_RC(enc(&e, 0, 0, chunk, cl, &out, &why), MOQ_OK);
    CHECK_BYTES(out.buf, out.len, want, wl);
    lm_enc_close(&e);

    /* Per-sample values carried in trun where tfhd would do: the same
     * effective values give the same Object (section 11.1). */
    {
        static const uint32_t durs[3] = { 1024, 1024, 1024 };
        static const uint32_t sizes[3] = { 6, 5, 4 };
        static const uint32_t flags[3] = { 0x02000000, 0x02000000, 0x02000000 };
        static const int64_t ctos[3] = { 0, 0, 0 };
        uint8_t pl[16];
        lm_fill(pl, 15, 0x60);
        lm_chunk_spec_t s;
        memset(&s, 0, sizeof(s));
        s.track_id = 1; s.tfdt_version = 1; s.bmdt = 2048;
        s.trun_version = 1;
        s.trun_flags = 0x001 | 0x100 | 0x200 | 0x400 | 0x800;
        s.n = 3; s.durs = durs; s.sizes = sizes; s.flags = flags; s.ctos = ctos;
        s.payload = pl; s.payload_len = 15;
        cl = lm_build_chunk(&s, chunk, sizeof(chunk));
        moq_locmaf_track_t ta = lm_track_a();
        CHECK_RC(lm_enc_open(&e, &ta, 0, 0, 0), MOQ_OK);
        CHECK_RC(enc(&e, 0, 0, chunk, cl, &out, &why), MOQ_OK);
        wl = lm_fixture_object(&lm_sequences[2].items[0], want, sizeof(want));
        CHECK_BYTES(out.buf, out.len, want, wl);
        lm_enc_close(&e);
    }
}

static void test_full_header_triggers(void)
{
    moq_locmaf_track_t ta = lm_track_a();
    uint8_t c1[256], c2[256];
    const lm_fixture_t *f1 = &lm_sequences[0].items[0], *f2 = &lm_sequences[0].items[1];
    size_t l1 = lm_fixture_canonical(f1, c1, sizeof(c1));
    size_t l2 = lm_fixture_canonical(f2, c2, sizeof(c2));
    enc_out_t out;
    moq_locmaf_reason_t why;
    lm_enc_t e;

    /* BMDT jump: F2 with a BMDT that does not continue F1's */
    CHECK_RC(lm_enc_open(&e, &ta, 0, 0, 0), MOQ_OK);
    CHECK_RC(enc(&e, 0, 0, c1, l1, &out, &why), MOQ_OK);
    moq_locmaf_encoder_commit(&e.e);
    uint8_t jump[256];
    memcpy(jump, c2, l2);
    jump[70] = 0x05;                         /* tfdt 0x400 -> 0x500 */
    CHECK_RC(enc(&e, 0, 1, jump, l2, &out, &why), MOQ_OK);
    CHECK(out.buf[0] == MOQ_LOCMAF_ELEMENT_FULL_HEADER);
    lm_enc_close(&e);

    /* gap */
    CHECK_RC(lm_enc_open(&e, &ta, 0, 0, 0), MOQ_OK);
    CHECK_RC(enc(&e, 0, 0, c1, l1, &out, &why), MOQ_OK);
    moq_locmaf_encoder_commit(&e.e);
    CHECK_RC(enc(&e, 0, 2, c2, l2, &out, &why), MOQ_OK);
    CHECK(out.buf[0] == MOQ_LOCMAF_ELEMENT_FULL_HEADER);
    /* without commit nothing moved: the next Location is still a delta */
    CHECK_RC(enc(&e, 0, 1, c2, l2, &out, &why), MOQ_OK);
    CHECK(out.buf[0] == MOQ_LOCMAF_ELEMENT_DELTA_HEADER);
    lm_enc_close(&e);

    /* new group, after rawBoxes, after reset */
    CHECK_RC(lm_enc_open(&e, &ta, 0, 0, 0), MOQ_OK);
    CHECK_RC(enc(&e, 0, 0, c1, l1, &out, &why), MOQ_OK);
    moq_locmaf_encoder_commit(&e.e);
    CHECK_RC(enc(&e, 1, 0, c2, l2, &out, &why), MOQ_OK);
    CHECK(out.buf[0] == MOQ_LOCMAF_ELEMENT_FULL_HEADER);
    CHECK_RC(moq_locmaf_encode_raw_boxes(&e.e, 0, 1, (moq_bytes_t){ c1, l1 }, out.buf,
                                         sizeof(out.buf), &out.len, &why), MOQ_OK);
    CHECK(out.len == l1 + 1 && out.buf[0] == MOQ_LOCMAF_ELEMENT_RAW_BOXES);
    CHECK(memcmp(out.buf + 1, c1, l1) == 0);
    moq_locmaf_encoder_commit(&e.e);
    CHECK_RC(enc(&e, 0, 2, c2, l2, &out, &why), MOQ_OK);
    CHECK(out.buf[0] == MOQ_LOCMAF_ELEMENT_FULL_HEADER);
    moq_locmaf_encoder_commit(&e.e);
    moq_locmaf_encoder_reset(&e.e);
    moq_locmaf_encoder_reset(NULL);
    CHECK_RC(enc(&e, 0, 3, c2, l2, &out, &why), MOQ_OK);
    CHECK(out.buf[0] == MOQ_LOCMAF_ELEMENT_FULL_HEADER);
    lm_enc_close(&e);
}

static void test_two_phase(void)
{
    moq_locmaf_track_t ta = lm_track_a();
    uint8_t c1[256], c2[256], c4[256];
    size_t l1 = lm_fixture_canonical(&lm_sequences[0].items[0], c1, sizeof(c1));
    size_t l2 = lm_fixture_canonical(&lm_sequences[0].items[1], c2, sizeof(c2));
    size_t l4 = lm_fixture_canonical(&lm_sequences[2].items[0], c4, sizeof(c4));
    enc_out_t a, b;
    moq_locmaf_reason_t why;
    lm_enc_t e;
    CHECK_RC(lm_enc_open(&e, &ta, 0, 0, 0), MOQ_OK);
    CHECK_RC(enc(&e, 0, 0, c1, l1, &a, &why), MOQ_OK);
    moq_locmaf_encoder_commit(&e.e);
    /* retry before commit is byte-identical */
    CHECK_RC(enc(&e, 0, 1, c2, l2, &a, &why), MOQ_OK);
    CHECK_RC(enc(&e, 0, 1, c2, l2, &b, &why), MOQ_OK);
    CHECK_BYTES(a.buf, a.len, b.buf, b.len);
    /* BUFFER: the exact length, nothing staged in its place */
    size_t len = 0;
    CHECK_RC(moq_locmaf_encode_chunk(&e.e, 0, 1, (moq_bytes_t){ c2, l2 }, NULL, 0, &len,
                                     &why), MOQ_ERR_BUFFER);
    CHECK_EQ(len, a.len);
    CHECK_RC(moq_locmaf_encode_chunk(&e.e, 0, 1, (moq_bytes_t){ c2, l2 }, b.buf, a.len - 1,
                                     &len, &why), MOQ_ERR_BUFFER);
    /* check() between encode and commit leaves the stage alone */
    size_t bound;
    CHECK_RC(moq_locmaf_encoder_check(&e.e, (moq_bytes_t){ c4, l4 }, &bound, &why), MOQ_OK);
    CHECK(bound >= 15);
    moq_locmaf_encoder_commit(&e.e);
    /* the committed reference is F2's: an identical next chunk is empty */
    uint8_t c3[256];
    memcpy(c3, c2, l2);
    c3[70] = 0x08;                           /* tfdt 0x800 = 1024 + 1024 */
    CHECK_RC(enc(&e, 0, 2, c3, l2, &a, &why), MOQ_OK);
    uint8_t empty[] = { 3, 0, 0xCA, 0xFE, 0xBA, 0xBE };
    CHECK_BYTES(a.buf, a.len, empty, sizeof(empty));
    moq_locmaf_encoder_stats_t st = estats(&e);
    CHECK_EQ(st.stages_replaced, 1);
    CHECK_EQ(st.stages_committed, 2);
    CHECK_EQ(st.chunks_encoded, 4);
    CHECK_EQ(st.full_headers_written, 1);
    CHECK_EQ(st.delta_headers_written, 3);
    /* commit with nothing staged changes nothing */
    moq_locmaf_encoder_commit(&e.e);
    moq_locmaf_encoder_commit(&e.e);
    CHECK_EQ(estats(&e).stages_committed, 3);
    moq_locmaf_encoder_commit(NULL);
    /* an object_id at or below the committed position */
    CHECK_RC(enc(&e, 0, 2, c3, l2, &a, &why), MOQ_ERR_INVAL);
    CHECK_RC(enc(&e, 0, 0, c3, l2, &a, &why), MOQ_ERR_INVAL);
    CHECK_RC(moq_locmaf_encode_raw_boxes(&e.e, 0, 1, (moq_bytes_t){ c1, l1 }, a.buf,
                                         sizeof(a.buf), &a.len, &why), MOQ_ERR_INVAL);
    /* arguments */
    CHECK_RC(moq_locmaf_encode_chunk(NULL, 0, 9, (moq_bytes_t){ c1, l1 }, a.buf, 10, &len,
                                     &why), MOQ_ERR_INVAL);
    CHECK_RC(moq_locmaf_encode_chunk(&e.e, 0, 9, (moq_bytes_t){ c1, l1 }, a.buf, 10, NULL,
                                     &why), MOQ_ERR_INVAL);
    CHECK_RC(moq_locmaf_encode_chunk(&e.e, 0, 9, (moq_bytes_t){ NULL, 1 }, a.buf, 10, &len,
                                     &why), MOQ_ERR_INVAL);
    CHECK_RC(moq_locmaf_encode_chunk(&e.e, 0, 9, (moq_bytes_t){ c1, l1 }, NULL, 10, &len,
                                     &why), MOQ_ERR_INVAL);
    CHECK_RC(moq_locmaf_encoder_check(NULL, (moq_bytes_t){ c1, l1 }, &bound, &why),
             MOQ_ERR_INVAL);
    /* UINT64_MAX ends a group's positions */
    CHECK_RC(enc(&e, 4, UINT64_MAX, c1, l1, &a, &why), MOQ_OK);
    moq_locmaf_encoder_commit(&e.e);
    CHECK_RC(enc(&e, 4, 0, c1, l1, &a, &why), MOQ_ERR_INVAL);
    lm_enc_close(&e);
}

/* The section 11.2 rules for sizes, on the encoder side. */
static void test_size_rules(void)
{
    moq_locmaf_track_t ta = lm_track_a();
    lm_enc_t e;
    enc_out_t out;
    moq_locmaf_reason_t why;
    uint8_t chunk[512], pl[64];
    lm_fill(pl, sizeof(pl), 1);
    static const uint32_t uni[3] = { 7, 7, 7 };
    lm_chunk_spec_t s;
    memset(&s, 0, sizeof(s));
    s.track_id = 1; s.tfdt_version = 1; s.trun_flags = 0x201;
    s.n = 3; s.sizes = uni; s.payload = pl; s.payload_len = 21;
    size_t cl = lm_build_chunk(&s, chunk, sizeof(chunk));
    CHECK_RC(lm_enc_open(&e, &ta, 0, 0, 0), MOQ_OK);
    CHECK_RC(enc(&e, 0, 0, chunk, cl, &out, &why), MOQ_OK);
    /* uniform n > 1: ID 6, never ID 1 */
    uint8_t want[] = { 2, 6, 6, 7, 0x0A, 0, 0x0E, 3 };
    CHECK_BYTES(out.buf, out.len - 21, want, sizeof(want));
    lm_enc_close(&e);
    /* n = 1: neither */
    s.n = 1; s.payload_len = 7;
    cl = lm_build_chunk(&s, chunk, sizeof(chunk));
    CHECK_RC(lm_enc_open(&e, &ta, 0, 0, 0), MOQ_OK);
    CHECK_RC(enc(&e, 0, 0, chunk, cl, &out, &why), MOQ_OK);
    uint8_t want1[] = { 2, 4, 0x0A, 0, 0x0E, 1 };
    CHECK_BYTES(out.buf, out.len - 7, want1, sizeof(want1));
    lm_enc_close(&e);
}

/* -- Refusals ------------------------------------------------------------------ */

static void expect_refused(const moq_locmaf_track_t *t, const uint8_t *chunk, size_t len,
                           moq_result_t rc, moq_locmaf_reason_t r, const char *what)
{
    lm_enc_t e;
    CHECK_RC(lm_enc_open(&e, t, 4, 4, 2), MOQ_OK);
    enc_out_t out;
    moq_locmaf_reason_t why;
    size_t bound = 7;
    moq_result_t c = moq_locmaf_encoder_check(&e.e, (moq_bytes_t){ chunk, len }, &bound, &why);
    moq_result_t got = enc(&e, 0, 0, chunk, len, &out, &why);
    if (got != rc || why != r)
        fprintf(stderr, "  refusal %s: %s\n", what, moq_locmaf_reason_name(why));
    CHECK_RC(got, rc);
    CHECK_WHY(why, r);
    CHECK_RC(c, rc);                          /* check() agrees */
    CHECK_EQ(bound, 7);
    moq_locmaf_encoder_stats_t st = estats(&e);
    CHECK_EQ(st.chunks_refused, 2);
    CHECK_WHY(st.last_reason, r);
    CHECK_EQ(st.chunks_encoded, 0);
    moq_locmaf_encoder_commit(&e.e);
    CHECK_EQ(estats(&e).stages_committed, 0);
    lm_enc_close(&e);
}

static lm_chunk_spec_t base_spec(uint8_t *payload)
{
    lm_chunk_spec_t s;
    memset(&s, 0, sizeof(s));
    s.track_id = 1;
    s.tfdt_version = 1;
    s.tfhd_flags = 0x020010;
    s.tfhd_size = 4;
    s.trun_flags = 0x001;
    s.n = 1;
    lm_fill(payload, 4, 9);
    s.payload = payload;
    s.payload_len = 4;
    return s;
}

/* Patch the first occurrence of a box type's 4CC bytes + offset. */
static void patch(uint8_t *buf, size_t len, const char *type, size_t off, uint8_t v)
{
    for (size_t i = 0; i + 4 <= len; i++)
        if (memcmp(buf + i, type, 4) == 0) { buf[i + off] = v; return; }
    fprintf(stderr, "patch: no %s\n", type);
    exit(2);
}

static void test_refusals(void)
{
    moq_locmaf_track_t ta = lm_track_a(), p8 = lm_track_p8();
    uint8_t pl[8], c[1024], b[256];
    size_t n, k;
    lm_chunk_spec_t s;
    const moq_result_t P = MOQ_ERR_PROTO, U = MOQ_ERR_UNSUPPORTED, L = MOQ_ERR_LIMIT;

    /* the base chunk encodes */
    s = base_spec(pl);
    n = lm_build_chunk(&s, c, sizeof(c));
    {
        lm_enc_t e;
        CHECK_RC(lm_enc_open(&e, &ta, 0, 0, 0), MOQ_OK);
        enc_out_t out;
        moq_locmaf_reason_t why;
        CHECK_RC(enc(&e, 0, 0, c, n, &out, &why), MOQ_OK);
        lm_enc_close(&e);
    }

    /* PROTO: malformed CMAF and the track_ID */
    s = base_spec(pl); n = lm_build_chunk(&s, c, sizeof(c));
    patch(c, n, "mfhd", 0, 'x');                   /* mfhd becomes an unknown box */
    expect_refused(&ta, c, n, U, MOQ_LOCMAF_REASON_UNKNOWN_BOX, "mfhd renamed");
    /* no mfhd: the moof shrinks by its 16 bytes */
    s = base_spec(pl); n = lm_build_chunk(&s, c, sizeof(c));
    memmove(c + 8, c + 24, n - 24);
    n -= 16;
    c[3] = (uint8_t)(c[3] - 16);
    expect_refused(&ta, c, n, P, MOQ_LOCMAF_REASON_MALFORMED_CMAF, "no mfhd");
    s = base_spec(pl); n = lm_build_chunk(&s, c, sizeof(c));
    c[3] = (uint8_t)(c[3] + 1);                    /* moof overruns */
    expect_refused(&ta, c, n, P, MOQ_LOCMAF_REASON_MALFORMED_CMAF, "moof overrun");
    s = base_spec(pl); s.trun_flags = 0x405; s.n = 1;
    { static const uint32_t f[1] = { 0 }; s.flags = f; n = lm_build_chunk(&s, c, sizeof(c)); }
    expect_refused(&ta, c, n, P, MOQ_LOCMAF_REASON_MALFORMED_CMAF, "first + per-sample flags");
    s = base_spec(pl); s.tfhd_size = 5; n = lm_build_chunk(&s, c, sizeof(c));
    expect_refused(&ta, c, n, P, MOQ_LOCMAF_REASON_MALFORMED_CMAF, "sizes past the payload");
    s = base_spec(pl); s.track_id = 2; n = lm_build_chunk(&s, c, sizeof(c));
    expect_refused(&ta, c, n, P, MOQ_LOCMAF_REASON_TRACK_ID, "track_ID");
    {
        static const uint8_t styp[8] = { 'c', 'm', 'f', 's', 0, 0, 0, 0 };
        lm_gen_box_t g[2] = { { LM_FOURCC_T('s','t','y','p'), styp, 8 },
                              { LM_FOURCC_T('s','t','y','p'), styp, 8 } };
        s = base_spec(pl); s.gen_boxes = g; s.gen_box_count = 2;
        n = lm_build_chunk(&s, c, sizeof(c));
        expect_refused(&ta, c, n, P, MOQ_LOCMAF_REASON_MALFORMED_CMAF, "two styp");
        s.gen_box_count = 1; g[0].len = 7;
        n = lm_build_chunk(&s, c, sizeof(c));
        expect_refused(&ta, c, n, P, MOQ_LOCMAF_REASON_MALFORMED_CMAF, "bad styp");
        g[0].type = LM_FOURCC_T('a','b','c','d');
        n = lm_build_chunk(&s, c, sizeof(c));
        expect_refused(&ta, c, n, U, MOQ_LOCMAF_REASON_GEN_BOX_UNSUPPORTED, "abcd before moof");
        g[0].type = LM_FOURCC_T('f','t','y','p'); g[0].len = 8;
        n = lm_build_chunk(&s, c, sizeof(c));
        expect_refused(&ta, c, n, U, MOQ_LOCMAF_REASON_STRUCTURE, "ftyp before moof");
        /* more genBoxes than the limit (2) */
        static const uint8_t none[1] = { 0 };
        lm_gen_box_t g3[3] = { { LM_FOURCC_T('f','r','e','e'), none, 0 },
                               { LM_FOURCC_T('f','r','e','e'), none, 0 },
                               { LM_FOURCC_T('f','r','e','e'), none, 0 } };
        s = base_spec(pl); s.gen_boxes = g3; s.gen_box_count = 3;
        n = lm_build_chunk(&s, c, sizeof(c));
        expect_refused(&ta, c, n, L, MOQ_LOCMAF_REASON_LIMIT, "genBoxes past the limit");
    }

    /* UNSUPPORTED: outside the field model */
    s = base_spec(pl); n = lm_build_chunk(&s, c, sizeof(c));
    memcpy(c + n, c, n);
    expect_refused(&ta, c, 2 * n, U, MOQ_LOCMAF_REASON_MULTI_CHUNK, "two chunks");
    memcpy(c + n, "\0\0\0\x08" "free", 8);
    expect_refused(&ta, c, n + 8, U, MOQ_LOCMAF_REASON_STRUCTURE, "a box after the mdat");
    expect_refused(&ta, c, n - 12, U, MOQ_LOCMAF_REASON_STRUCTURE, "no mdat");
    k = lm_hex("0000000C 6D646174 01020304", b, sizeof(b));
    memcpy(b + k, c, n);
    expect_refused(&ta, b, k + n, U, MOQ_LOCMAF_REASON_STRUCTURE, "mdat before moof");
    s = base_spec(pl); s.tfhd_flags |= 0x01; n = lm_build_chunk(&s, c, sizeof(c));
    expect_refused(&ta, c, n, U, MOQ_LOCMAF_REASON_DATA_LAYOUT, "base-data-offset");
    s = base_spec(pl); s.tfhd_flags |= 0x010000; n = lm_build_chunk(&s, c, sizeof(c));
    expect_refused(&ta, c, n, U, MOQ_LOCMAF_REASON_STRUCTURE, "duration-is-empty");
    s = base_spec(pl); s.trun_flags = 0; n = lm_build_chunk(&s, c, sizeof(c));
    expect_refused(&ta, c, n, U, MOQ_LOCMAF_REASON_DATA_LAYOUT, "no data offset");
    s = base_spec(pl); s.data_offset_override = true; s.data_offset = 7;
    n = lm_build_chunk(&s, c, sizeof(c));
    expect_refused(&ta, c, n, U, MOQ_LOCMAF_REASON_DATA_LAYOUT, "data offset elsewhere");
    s = base_spec(pl); s.tfhd_size = 3; n = lm_build_chunk(&s, c, sizeof(c));
    expect_refused(&ta, c, n, U, MOQ_LOCMAF_REASON_DATA_LAYOUT, "samples short of the payload");
    s = base_spec(pl); s.n = 0; n = lm_build_chunk(&s, c, sizeof(c));
    expect_refused(&ta, c, n, U, MOQ_LOCMAF_REASON_DATA_LAYOUT, "no samples, a payload");
    s = base_spec(pl); s.n = 5; s.payload_len = 0; s.tfhd_size = 0;
    n = lm_build_chunk(&s, c, sizeof(c));
    expect_refused(&ta, c, n, L, MOQ_LOCMAF_REASON_LIMIT, "samples past the limit");
    k = lm_hex("0000001C 73626770 00000000 726F6C6C 00000001 00000001 00000001", b, sizeof(b));
    s = base_spec(pl); s.traf_extra = b; s.traf_extra_len = k;
    n = lm_build_chunk(&s, c, sizeof(c));
    expect_refused(&ta, c, n, U, MOQ_LOCMAF_REASON_SAMPLE_GROUPS, "sbgp");
    k = lm_hex("0000001A 73677064 01000000 726F6C6C 00000002 00000001 FFFF", b, sizeof(b));
    s.traf_extra_len = k; n = lm_build_chunk(&s, c, sizeof(c));
    expect_refused(&ta, c, n, U, MOQ_LOCMAF_REASON_SAMPLE_GROUPS, "sgpd");
    k = lm_hex("0000001E 73756273 00000000 00000001 00000001 0001 0004 00 00 00000000",
               b, sizeof(b));
    s.traf_extra_len = k; n = lm_build_chunk(&s, c, sizeof(c));
    expect_refused(&ta, c, n, U, MOQ_LOCMAF_REASON_SUBS, "subs");
    k = lm_hex("00000018 75756964 000102030405060708090A0B0C0D0E0F", b, sizeof(b));
    s.traf_extra_len = k; n = lm_build_chunk(&s, c, sizeof(c));
    expect_refused(&ta, c, n, U, MOQ_LOCMAF_REASON_UNKNOWN_BOX, "uuid in traf");
    s.traf_extra = c; s.traf_extra_len = 0;
    k = lm_hex("00000022 70737368 00000000 1077EFECC0B24D02ACE33C1E52E2FB4B 00000002 ABCD",
               b, sizeof(b));
    s = base_spec(pl); s.moof_extra = b; s.moof_extra_len = k;
    n = lm_build_chunk(&s, c, sizeof(c));
    expect_refused(&ta, c, n, U, MOQ_LOCMAF_REASON_FRAGMENT_PSSH, "pssh in moof");
    k = lm_hex("00000018 74726166 00000010 74666864 00020000 00000001", b, sizeof(b));
    s.moof_extra_len = k; n = lm_build_chunk(&s, c, sizeof(c));
    expect_refused(&ta, c, n, U, MOQ_LOCMAF_REASON_STRUCTURE, "two traf");
    s = base_spec(pl); n = lm_build_chunk(&s, c, sizeof(c));
    patch(c, n, "tfdt", 0, 'x');
    expect_refused(&ta, c, n, U, MOQ_LOCMAF_REASON_UNKNOWN_BOX, "tfdt renamed");
    /* size escapes */
    s = base_spec(pl); n = lm_build_chunk(&s, c, sizeof(c));
    k = lm_hex("00000001 66726565 0000000000000010", b, sizeof(b));
    memcpy(b + k, c, n);
    expect_refused(&ta, b, k + n, U, MOQ_LOCMAF_REASON_SIZE_ESCAPE, "largesize");
    {
        s = base_spec(pl); n = lm_build_chunk(&s, c, sizeof(c));
        for (size_t i = 4; i + 4 <= n; i++)
            if (memcmp(c + i, "trun", 4) == 0) { memset(c + i - 4, 0, 4); break; }
        expect_refused(&ta, c, n, U, MOQ_LOCMAF_REASON_SIZE_ESCAPE, "nested size 0");
    }

    /* CENC */
    {
        static const uint8_t ivs[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
        static const uint16_t counts[1] = { 1 }, clear[1] = { 1 };
        static const uint32_t prot[1] = { 3 };
        /* a protected track with a nonzero IV size: no senc, no IVs to carry */
        moq_locmaf_track_t p16 = lm_track_p16(), p0 = lm_track_p0();
        s = base_spec(pl);
        n = lm_build_chunk(&s, c, sizeof(c));
        expect_refused(&p8, c, n, U, MOQ_LOCMAF_REASON_AUX_INFO, "IV size 8, no senc");
        expect_refused(&p16, c, n, U, MOQ_LOCMAF_REASON_AUX_INFO, "IV size 16, no senc");
        {
            lm_enc_t e0;
            CHECK_RC(lm_enc_open(&e0, &p0, 0, 0, 0), MOQ_OK);
            enc_out_t out0;
            moq_locmaf_reason_t why0;
            CHECK_RC(enc(&e0, 0, 0, c, n, &out0, &why0), MOQ_OK);   /* IV size 0 */
            lm_enc_close(&e0);
        }
        s = base_spec(pl); s.senc = true; s.iv = 8; s.ivs = ivs;
        n = lm_build_chunk(&s, c, sizeof(c));
        expect_refused(&ta, c, n, P, MOQ_LOCMAF_REASON_CLEAR_TRACK_CENC, "senc on a clear track");
        patch(c, n, "senc", 4, 1);                 /* version 1 */
        expect_refused(&p8, c, n, U, MOQ_LOCMAF_REASON_AUX_INFO, "senc version 1");
        n = lm_build_chunk(&s, c, sizeof(c));
        patch(c, n, "senc", 7, 4);                 /* an unknown flag */
        expect_refused(&p8, c, n, U, MOQ_LOCMAF_REASON_AUX_INFO, "senc flags");
        s.iv = 7; n = lm_build_chunk(&s, c, sizeof(c));
        expect_refused(&p8, c, n, P, MOQ_LOCMAF_REASON_MALFORMED_CMAF, "senc IV size");
        s.iv = 8; s.saiz_saio = true; n = lm_build_chunk(&s, c, sizeof(c));
        lm_enc_t e;
        CHECK_RC(lm_enc_open(&e, &p8, 0, 0, 0), MOQ_OK);
        enc_out_t out;
        moq_locmaf_reason_t why;
        CHECK_RC(enc(&e, 0, 0, c, n, &out, &why), MOQ_OK);   /* saiz and saio agree */
        lm_enc_close(&e);
        patch(c, n, "saio", 15, 0x7F);             /* saio offset elsewhere */
        expect_refused(&p8, c, n, U, MOQ_LOCMAF_REASON_AUX_INFO, "saio offset");
        n = lm_build_chunk(&s, c, sizeof(c));
        patch(c, n, "saiz", 8, 9);                 /* saiz default 9, aux 8 */
        expect_refused(&p8, c, n, U, MOQ_LOCMAF_REASON_AUX_INFO, "saiz size");
        s.sub_counts = counts; s.clear = clear; s.prot = prot; s.saiz_saio = false;
        n = lm_build_chunk(&s, c, sizeof(c));
        CHECK_RC(lm_enc_open(&e, &p8, 0, 0, 0), MOQ_OK);
        CHECK_RC(enc(&e, 0, 0, c, n, &out, &why), MOQ_OK);   /* 1 + 3 = 4 */
        lm_enc_close(&e);
        static const uint32_t prot2[1] = { 2 };
        s.prot = prot2; n = lm_build_chunk(&s, c, sizeof(c));
        expect_refused(&p8, c, n, P, MOQ_LOCMAF_REASON_MALFORMED_CMAF, "subsample sum");
        static const uint16_t counts5[1] = { 5 }, clear5[5] = { 0 };
        static const uint32_t prot5[5] = { 4, 0, 0, 0, 0 };
        s.sub_counts = counts5; s.clear = clear5; s.prot = prot5;
        n = lm_build_chunk(&s, c, sizeof(c));
        expect_refused(&p8, c, n, L, MOQ_LOCMAF_REASON_LIMIT, "subsamples past the limit");
        /* saiz and saio without a senc */
        s = base_spec(pl); s.senc = true; s.iv = 8; s.ivs = ivs; s.saiz_saio = true;
        n = lm_build_chunk(&s, c, sizeof(c));
        patch(c, n, "senc", 0, 'x');
        expect_refused(&p8, c, n, U, MOQ_LOCMAF_REASON_UNKNOWN_BOX, "senc renamed");
    }

    /* rawBoxes: the decoder's rule */
    {
        lm_enc_t e;
        CHECK_RC(lm_enc_open(&e, &ta, 0, 0, 0), MOQ_OK);
        enc_out_t out;
        moq_locmaf_reason_t why;
        k = lm_hex("00000007 66726565", b, sizeof(b));
        CHECK_RC(moq_locmaf_encode_raw_boxes(&e.e, 0, 0, (moq_bytes_t){ b, k }, out.buf,
                 sizeof(out.buf), &out.len, &why), MOQ_ERR_PROTO);
        CHECK_WHY(why, MOQ_LOCMAF_REASON_RAW_BOXES_MALFORMED);
        k = lm_hex("00000010 66747970 636D6663 00000000 00000008 6D6F6F76", b, sizeof(b));
        CHECK_RC(moq_locmaf_encode_raw_boxes(&e.e, 0, 0, (moq_bytes_t){ b, k }, out.buf,
                 sizeof(out.buf), &out.len, &why), MOQ_ERR_UNSUPPORTED);
        CHECK_WHY(why, MOQ_LOCMAF_REASON_RAW_BOXES_UNSUPPORTED);
        CHECK_RC(moq_locmaf_encode_raw_boxes(&e.e, 0, 0, (moq_bytes_t){ NULL, 0 }, out.buf,
                 sizeof(out.buf), &out.len, &why), MOQ_ERR_PROTO);
        s = base_spec(pl); n = lm_build_chunk(&s, c, sizeof(c));
        size_t len = 0;
        CHECK_RC(moq_locmaf_encode_raw_boxes(&e.e, 0, 0, (moq_bytes_t){ c, n }, NULL, 0, &len,
                 &why), MOQ_ERR_BUFFER);
        CHECK_EQ(len, n + 1);
        moq_locmaf_encoder_stats_t st = estats(&e);
        CHECK_EQ(st.chunks_refused, 3);
        CHECK_EQ(st.raw_boxes_written, 0);
        lm_enc_close(&e);
    }
}

int main(void)
{
    test_fixture_objects();
    test_noncanonical_sources();
    test_full_header_triggers();
    test_two_phase();
    test_size_rules();
    test_refusals();
    return lm_finish("locmaf_encode");
}
