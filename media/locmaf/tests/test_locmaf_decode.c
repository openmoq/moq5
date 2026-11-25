/*
 * moq_locmaf_decoder_*() on clear tracks: the golden fixtures, sequencing
 * (sections 3, 7.1, 9.3), the reference chain (12.1 to 12.4), sample sizes
 * (11.2), effective values (15.1), refusals with their reasons, checked
 * arithmetic at every boundary, the decoder counters, and the track facts
 * taken from a CMAF Header.
 */
#include "locmaf_fixtures.h"
#include "media_builders.h"

#include <moq/vi64.h>

/* -- Object builder ---------------------------------------------------------- */

typedef struct bld {
    uint8_t *b;
    size_t   n, cap;
} bld_t;

static bld_t b_new(size_t cap)
{
    bld_t b = { malloc(cap), 0, cap };
    if (!b.b) { fprintf(stderr, "out of memory\n"); exit(2); }
    return b;
}

static void b_vi(bld_t *b, uint64_t v)
{
    size_t k = moq_vi64_encode(v, b->b + b->n, b->cap - b->n);
    if (!k) { fprintf(stderr, "builder overflow\n"); exit(2); }
    b->n += k;
}

static void b_scalar(bld_t *b, unsigned id, uint64_t v) { b_vi(b, id); b_vi(b, v); }

static void b_list(bld_t *b, unsigned id, const uint64_t *v, size_t n)
{
    size_t len = 0;
    for (size_t i = 0; i < n; i++) len += moq_vi64_len(v[i]);
    b_vi(b, id);
    b_vi(b, len);
    for (size_t i = 0; i < n; i++) b_vi(b, v[i]);
}

static void b_hex(bld_t *b, const char *hex)
{
    b->n += lm_hex(hex, b->b + b->n, b->cap - b->n);
}

/* type, properties_length, the property block, then payload_len bytes. */
static size_t b_object(uint8_t *out, size_t cap, unsigned kind, const bld_t *props,
                       size_t payload_len)
{
    bld_t o = { out, 0, cap };
    b_vi(&o, kind);
    b_vi(&o, props->n);
    if (props->n > cap - o.n) { fprintf(stderr, "object overflow\n"); exit(2); }
    memcpy(o.b + o.n, props->b, props->n);
    o.n += props->n;
    if (payload_len > cap - o.n) { fprintf(stderr, "object overflow\n"); exit(2); }
    lm_fill(o.b + o.n, payload_len, 0x5A);
    return o.n + payload_len;
}

/* Push an object built from props hex. */
static moq_result_t push_hex(lm_dec_t *dec, uint64_t g, uint64_t o, unsigned kind,
                             const char *props_hex, size_t payload_len,
                             moq_locmaf_chunk_t *ch, moq_locmaf_reason_t *why)
{
    uint8_t pb[1024], obj[2048];
    bld_t p = { pb, 0, sizeof(pb) };
    b_hex(&p, props_hex);
    size_t n = b_object(obj, sizeof(obj), kind, &p, payload_len);
    *why = (moq_locmaf_reason_t)0xEEEE;
    return moq_locmaf_decoder_push(&dec->d, g, o, (moq_bytes_t){ obj, n }, ch,
                                   sizeof(*ch), why);
}

static moq_result_t push_bld(lm_dec_t *dec, uint64_t g, uint64_t o, unsigned kind,
                             const bld_t *props, size_t payload_len,
                             moq_locmaf_chunk_t *ch, moq_locmaf_reason_t *why)
{
    size_t cap = props->n + payload_len + 32;
    uint8_t *obj = malloc(cap);
    if (!obj) { fprintf(stderr, "out of memory\n"); exit(2); }
    size_t n = b_object(obj, cap, kind, props, payload_len);
    *why = (moq_locmaf_reason_t)0xEEEE;
    moq_result_t rc = moq_locmaf_decoder_push(&dec->d, g, o, (moq_bytes_t){ obj, n },
                                              ch, sizeof(*ch), why);
    free(obj);
    return rc;
}

#define FULL  MOQ_LOCMAF_ELEMENT_FULL_HEADER
#define DELTA MOQ_LOCMAF_ELEMENT_DELTA_HEADER

static void open_dec(lm_dec_t *dec, const moq_locmaf_track_t *t)
{
    moq_result_t rc = lm_dec_open(dec, t, 0, 0, 0);
    CHECK_RC(rc, MOQ_OK);
    if (rc != MOQ_OK) { fprintf(stderr, "decoder open failed\n"); exit(1); }
}

static moq_locmaf_decoder_stats_t stats(const lm_dec_t *dec)
{
    moq_locmaf_decoder_stats_t s;
    memset(&s, 0, sizeof(s));
    CHECK_RC(moq_locmaf_decoder_get_stats(&dec->d, &s, sizeof(s)), MOQ_OK);
    return s;
}

/* -- Fixtures ------------------------------------------------------------------ */

typedef struct expect {
    moq_locmaf_object_kind_t kind;
    uint64_t bmdt;
    uint32_t n;
    uint32_t durs[3], sizes[3], flags[3];
    int64_t  ctos[3];
    size_t   gen_boxes;
} expect_t;

static const struct { const char *name; expect_t e; } expected[] = {
    { "F1",  { MOQ_LOCMAF_OBJECT_FULL, 0, 1, { 1024 }, { 4 }, { 0x02000000u }, { 0 }, 0 } },
    { "F2",  { MOQ_LOCMAF_OBJECT_DELTA, 1024, 1, { 1024 }, { 4 }, { 0x02000000u }, { 0 }, 0 } },
    { "F3A", { MOQ_LOCMAF_OBJECT_FULL, 900000, 1, { 3000 }, { 5 }, { 0x02000000u }, { 0 }, 0 } },
    { "F3B", { MOQ_LOCMAF_OBJECT_DELTA, 903000, 1, { 3000 }, { 7 }, { 0x01010000u }, { 0 }, 0 } },
    { "F3C", { MOQ_LOCMAF_OBJECT_DELTA, 906000, 1, { 3000 }, { 7 }, { 0x01010000u }, { 0 }, 0 } },
    { "F4",  { MOQ_LOCMAF_OBJECT_FULL, 2048, 3, { 1024, 1024, 1024 }, { 6, 5, 4 },
               { 0x02000000u, 0x02000000u, 0x02000000u }, { 0 }, 0 } },
    { "F4b", { MOQ_LOCMAF_OBJECT_DELTA, 5120, 3, { 1024, 1024, 1024 }, { 7, 5, 3 },
               { 0x02000000u, 0x02000000u, 0x02000000u }, { 0 }, 0 } },
    { "F5a", { MOQ_LOCMAF_OBJECT_FULL, 0, 2, { 1024, 1024 }, { 10, 20 },
               { 0x02000000u, 0x02000000u }, { 0 }, 0 } },
    { "F5b", { MOQ_LOCMAF_OBJECT_DELTA, 2048, 3, { 1024, 1024, 1024 }, { 10, 20, 30 },
               { 0x02000000u, 0x02000000u, 0x02000000u }, { 0 }, 0 } },
    { "F5c", { MOQ_LOCMAF_OBJECT_DELTA, 5120, 1, { 1024 }, { 5 }, { 0x02000000u }, { 0 }, 0 } },
    { "F6",  { MOQ_LOCMAF_OBJECT_FULL, 0, 3, { 3000, 3000, 3000 }, { 100, 50, 50 },
               { 0x02000000u, 0x01010000u, 0x01010000u }, { 3000, -3000, 0 }, 0 } },
    { "F7",  { MOQ_LOCMAF_OBJECT_FULL, 0, 1, { 1024 }, { 4 }, { 0x02000000u }, { 0 }, 2 } },
    { "F8",  { MOQ_LOCMAF_OBJECT_FULL, 96000, 0, { 0 }, { 0 }, { 0 }, { 0 }, 1 } },
};

static const expect_t *expect_for(const char *name)
{
    for (size_t i = 0; i < sizeof(expected) / sizeof(expected[0]); i++)
        if (strcmp(expected[i].name, name) == 0) return &expected[i].e;
    return NULL;
}

static bool within(const void *p, const void *base, size_t len)
{
    uintptr_t a = (uintptr_t)p, b = (uintptr_t)base;
    return a >= b && a - b <= len;
}

static void test_fixtures(void)
{
    for (size_t s = 0; s < lm_sequence_count; s++) {
        const lm_sequence_t *seq = &lm_sequences[s];
        moq_locmaf_track_t track = lm_fixture_track(&seq->items[0]);
        if (track.is_protected) continue;      /* CENC fixtures: locmaf_cenc */
        lm_dec_t dec;
        open_dec(&dec, &track);
        for (size_t i = 0; i < seq->count; i++) {
            const lm_fixture_t *f = &seq->items[i];
            const expect_t *e = expect_for(f->name);
            CHECK(e != NULL);
            if (!e) continue;
            uint8_t obj[512];
            size_t n = lm_fixture_object(f, obj, sizeof(obj));
            moq_locmaf_chunk_t ch;
            moq_locmaf_reason_t why;
            moq_result_t rc = moq_locmaf_decoder_push(&dec.d, 0, f->object_id,
                (moq_bytes_t){ obj, n }, &ch, sizeof(ch), &why);
            if (rc != MOQ_OK) fprintf(stderr, "  fixture %s\n", f->name);
            CHECK_RC(rc, MOQ_OK);
            CHECK_WHY(why, MOQ_LOCMAF_REASON_NONE);
            if (rc != MOQ_OK) continue;
            CHECK_EQ(ch.struct_size, MOQ_LOCMAF_CHUNK_V0_SIZE);
            CHECK_EQ(ch.kind, e->kind);
            CHECK_EQ(ch.base_media_decode_time, e->bmdt);
            CHECK_EQ(ch.sample_count, e->n);
            CHECK_EQ(ch.sample_description_index, 1);
            CHECK(!ch.has_aux_info && !ch.has_subsamples && ch.per_sample_iv_size == 0);
            CHECK(ch.ivs.len == 0 && ch.subsample_total == 0);
            uint64_t off = 0;
            for (uint32_t k = 0; k < e->n && k < 3; k++) {
                CHECK_EQ(ch.samples[k].duration, e->durs[k]);
                CHECK_EQ(ch.samples[k].size, e->sizes[k]);
                CHECK_EQ(ch.samples[k].flags, e->flags[k]);
                CHECK_EQ(ch.samples[k].composition_offset, e->ctos[k]);
                CHECK_EQ(ch.samples[k].offset, off);
                CHECK_EQ(ch.samples[k].subsample_count, 0);
                CHECK_EQ(ch.samples[k]._reserved, 0);
                off += ch.samples[k].size;
            }
            /* Borrows: tables in the decoder's storage, bytes in the Object. */
            if (e->n) CHECK(within(ch.samples, dec.storage, dec.storage_size));
            CHECK_EQ(ch.mdat_payload.len, f->payload_len);
            if (f->payload_len) CHECK(ch.mdat_payload.data == obj + n - f->payload_len);
            CHECK_EQ(ch.gen_box_count, e->gen_boxes);
            for (size_t k = 0; k < ch.gen_box_count; k++)
                CHECK(within(ch.gen_boxes[k].payload.data, obj, n));
            if (ch.gen_box_count)
                CHECK(within(ch.gen_boxes, dec.storage, dec.storage_size));
            CHECK(ch.raw_boxes.len == 0);
            CHECK(moq_locmaf_decoder_expects(&dec.d, 0, f->object_id + 1));
        }
        lm_dec_close(&dec);
    }
}

/* -- Sequencing ------------------------------------------------------------------ */

static void test_sequencing(void)
{
    moq_locmaf_track_t ta = lm_track_a();
    moq_locmaf_chunk_t ch;
    moq_locmaf_reason_t why;
    lm_dec_t dec;

    /* gap_resets: a gap discards the reference; the position still moves. */
    open_dec(&dec, &ta);
    CHECK_RC(push_hex(&dec, 0, 0, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_OK);
    CHECK_RC(push_hex(&dec, 0, 2, DELTA, "", 4, &ch, &why), MOQ_ERR_WRONG_STATE);
    CHECK_WHY(why, MOQ_LOCMAF_REASON_NO_REFERENCE);
    CHECK_RC(push_hex(&dec, 0, 3, DELTA, "", 4, &ch, &why), MOQ_ERR_WRONG_STATE);
    CHECK_RC(push_hex(&dec, 0, 2, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_ERR_INVAL);
    CHECK_RC(push_hex(&dec, 0, 4, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_OK);
    CHECK_RC(push_hex(&dec, 0, 5, DELTA, "", 4, &ch, &why), MOQ_OK);
    CHECK_EQ(ch.base_media_decode_time, 1024);
    lm_dec_close(&dec);

    /* new_group_resets and delta_first_in_group_wrong_state */
    open_dec(&dec, &ta);
    CHECK_RC(push_hex(&dec, 0, 3, DELTA, "", 4, &ch, &why), MOQ_ERR_WRONG_STATE);
    CHECK_WHY(why, MOQ_LOCMAF_REASON_NO_REFERENCE);
    CHECK_RC(push_hex(&dec, 0, 4, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_OK);
    CHECK(moq_locmaf_decoder_expects(&dec.d, 0, 5));
    CHECK(!moq_locmaf_decoder_expects(&dec.d, 1, 0));
    CHECK_RC(push_hex(&dec, 1, 0, DELTA, "", 4, &ch, &why), MOQ_ERR_WRONG_STATE);
    /* a lower group is a new group too */
    CHECK_RC(push_hex(&dec, 0, 0, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_OK);
    lm_dec_close(&dec);

    /* duplicate_is_inval_unchanged */
    open_dec(&dec, &ta);
    CHECK_RC(push_hex(&dec, 7, 0, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_OK);
    CHECK_RC(push_hex(&dec, 7, 1, DELTA, "", 4, &ch, &why), MOQ_OK);
    moq_locmaf_decoder_stats_t before = stats(&dec);
    memset(&ch, 0xEE, sizeof(ch));
    CHECK_RC(push_hex(&dec, 7, 1, DELTA, "", 4, &ch, &why), MOQ_ERR_INVAL);
    CHECK_WHY(why, MOQ_LOCMAF_REASON_NONE);
    CHECK(ch.struct_size == 0xEEEEEEEEu);
    CHECK_RC(push_hex(&dec, 7, 0, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_ERR_INVAL);
    moq_locmaf_decoder_stats_t after = stats(&dec);
    CHECK(memcmp(&before, &after, sizeof(before)) == 0);
    CHECK(moq_locmaf_decoder_expects(&dec.d, 7, 2));
    CHECK_RC(push_hex(&dec, 7, 2, DELTA, "", 4, &ch, &why), MOQ_OK);
    CHECK_EQ(ch.base_media_decode_time, 2048);
    lm_dec_close(&dec);

    /* full_mid_group_reanchors: a full header discards the delta state */
    {
        moq_locmaf_track_t tv = lm_track_v();
        open_dec(&dec, &tv);
        CHECK_RC(push_hex(&dec, 0, 0, FULL, "04 8B B8 08 E2 00 00 00 0A CD BB A0 0E 01",
                          5, &ch, &why), MOQ_OK);
        CHECK_RC(push_hex(&dec, 0, 1, DELTA, "1B 01 08", 7, &ch, &why), MOQ_OK);
        /* re-anchor with a duration only: flags fall back to trex */
        CHECK_RC(push_hex(&dec, 0, 2, FULL, "04 87 D0 0A E0 20 00 00 0E 01", 3, &ch, &why),
                 MOQ_OK);
        CHECK_EQ(ch.samples[0].duration, 2000);
        CHECK_EQ(ch.samples[0].flags, 0x01010000u);
        CHECK_EQ(ch.base_media_decode_time, 1u << 21);
        CHECK_RC(push_hex(&dec, 0, 3, DELTA, "", 3, &ch, &why), MOQ_OK);
        CHECK_EQ(ch.base_media_decode_time, (1u << 21) + 2000);
        CHECK_EQ(ch.samples[0].duration, 2000);
        lm_dec_close(&dec);
    }

    /* delta_after_raw_boxes_proto: rawBoxes delivers but anchors nothing */
    {
        uint8_t chunk[256];
        const lm_fixture_t *f1 = &lm_sequences[0].items[0];
        size_t cl = lm_fixture_canonical(f1, chunk + 1, sizeof(chunk) - 1);
        chunk[0] = 4;
        open_dec(&dec, &ta);
        CHECK_RC(push_hex(&dec, 0, 0, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_OK);
        CHECK_RC(moq_locmaf_decoder_push(&dec.d, 0, 1, (moq_bytes_t){ chunk, cl + 1 },
                                         &ch, sizeof(ch), &why), MOQ_OK);
        CHECK_EQ(ch.kind, MOQ_LOCMAF_OBJECT_RAW_BOXES);
        CHECK(ch.raw_boxes.data == chunk + 1 && ch.raw_boxes.len == cl);
        CHECK(ch.sample_count == 0 && ch.mdat_payload.len == 0 && ch.gen_box_count == 0);
        CHECK(!moq_locmaf_decoder_expects(&dec.d, 0, 2));
        CHECK_RC(push_hex(&dec, 0, 2, DELTA, "", 4, &ch, &why), MOQ_ERR_PROTO);
        CHECK_WHY(why, MOQ_LOCMAF_REASON_DELTA_AFTER_RAW_BOXES);
        CHECK_RC(push_hex(&dec, 0, 3, DELTA, "", 4, &ch, &why), MOQ_ERR_PROTO);
        CHECK_WHY(why, MOQ_LOCMAF_REASON_DELTA_AFTER_RAW_BOXES);
        CHECK_RC(push_hex(&dec, 0, 4, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_OK);
        CHECK_RC(push_hex(&dec, 0, 5, DELTA, "", 4, &ch, &why), MOQ_OK);
        /* rawBoxes first in a group, then a delta */
        CHECK_RC(moq_locmaf_decoder_push(&dec.d, 1, 0, (moq_bytes_t){ chunk, cl + 1 },
                                         &ch, sizeof(ch), &why), MOQ_OK);
        CHECK_RC(push_hex(&dec, 1, 1, DELTA, "", 4, &ch, &why), MOQ_ERR_PROTO);
        CHECK_WHY(why, MOQ_LOCMAF_REASON_DELTA_AFTER_RAW_BOXES);
        /* a gap after rawBoxes is plain loss: no reference */
        CHECK_RC(moq_locmaf_decoder_push(&dec.d, 1, 2, (moq_bytes_t){ chunk, cl + 1 },
                                         &ch, sizeof(ch), &why), MOQ_OK);
        CHECK_RC(push_hex(&dec, 1, 4, DELTA, "", 4, &ch, &why), MOQ_ERR_WRONG_STATE);
        lm_dec_close(&dec);
    }

    /* reset_then_full */
    open_dec(&dec, &ta);
    CHECK_RC(push_hex(&dec, 0, 0, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_OK);
    moq_locmaf_decoder_reset(&dec.d);
    moq_locmaf_decoder_reset(NULL);
    CHECK(!moq_locmaf_decoder_expects(&dec.d, 0, 1));
    CHECK_RC(push_hex(&dec, 0, 1, DELTA, "", 4, &ch, &why), MOQ_ERR_WRONG_STATE);
    CHECK_WHY(why, MOQ_LOCMAF_REASON_NO_REFERENCE);
    CHECK_RC(push_hex(&dec, 0, 2, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_OK);
    CHECK_RC(push_hex(&dec, 0, 3, DELTA, "", 4, &ch, &why), MOQ_OK);
    lm_dec_close(&dec);

    /* arith_object_id_max: UINT64_MAX ends the group's positions */
    open_dec(&dec, &ta);
    CHECK_RC(push_hex(&dec, 3, UINT64_MAX, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_OK);
    CHECK(!moq_locmaf_decoder_expects(&dec.d, 3, 0));
    CHECK_RC(push_hex(&dec, 3, UINT64_MAX, DELTA, "", 4, &ch, &why), MOQ_ERR_INVAL);
    CHECK_RC(push_hex(&dec, 3, 0, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_ERR_INVAL);
    CHECK_RC(push_hex(&dec, 4, 0, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_OK);
    lm_dec_close(&dec);

    /* Arguments */
    open_dec(&dec, &ta);
    {
        uint8_t obj[8] = { 2, 4, 0x0A, 0, 0x0E, 1, 0, 0 };
        CHECK_RC(moq_locmaf_decoder_push(NULL, 0, 0, (moq_bytes_t){ obj, 8 }, &ch,
                                         sizeof(ch), &why), MOQ_ERR_INVAL);
        CHECK_RC(moq_locmaf_decoder_push(&dec.d, 0, 0, (moq_bytes_t){ obj, 8 }, NULL,
                                         sizeof(ch), &why), MOQ_ERR_INVAL);
        CHECK_RC(moq_locmaf_decoder_push(&dec.d, 0, 0, (moq_bytes_t){ NULL, 8 }, &ch,
                                         sizeof(ch), &why), MOQ_ERR_INVAL);
        CHECK(!moq_locmaf_decoder_expects(NULL, 0, 0));
        /* nothing moved: object 0 still decodes */
        CHECK_RC(moq_locmaf_decoder_push(&dec.d, 0, 0, (moq_bytes_t){ obj, 8 }, &ch,
                                         sizeof(ch), NULL), MOQ_OK);
    }
    lm_dec_close(&dec);
}

/* -- Copy ------------------------------------------------------------------------- */

static void test_copy(void)
{
    moq_locmaf_track_t tv = lm_track_v();
    moq_locmaf_chunk_t ch;
    moq_locmaf_reason_t why;
    const lm_sequence_t *f3 = &lm_sequences[1];
    lm_dec_t fill, live;
    open_dec(&fill, &tv);
    open_dec(&live, &tv);
    for (size_t i = 0; i < f3->count; i++) {
        uint8_t obj[64];
        size_t n = lm_fixture_object(&f3->items[i], obj, sizeof(obj));
        CHECK_RC(moq_locmaf_decoder_push(&fill.d, 5, i, (moq_bytes_t){ obj, n }, &ch,
                                         sizeof(ch), &why), MOQ_OK);
    }
    /* copy_handoff: the live decoder continues where the fill stands */
    CHECK(!moq_locmaf_decoder_expects(&live.d, 5, 3));
    CHECK_RC(moq_locmaf_decoder_copy(&live.d, &fill.d), MOQ_OK);
    CHECK(moq_locmaf_decoder_expects(&live.d, 5, 3));
    CHECK(moq_locmaf_decoder_expects(&fill.d, 5, 3));
    CHECK_RC(push_hex(&live, 5, 3, DELTA, "", 7, &ch, &why), MOQ_OK);
    CHECK_EQ(ch.base_media_decode_time, 909000);
    CHECK_EQ(ch.samples[0].size, 7);
    CHECK_EQ(ch.samples[0].duration, 3000);
    /* the source is unchanged */
    CHECK_RC(push_hex(&fill, 5, 3, DELTA, "", 7, &ch, &why), MOQ_OK);
    CHECK_EQ(ch.base_media_decode_time, 909000);
    /* counters are not copied */
    CHECK_EQ(stats(&live).objects_decoded, 1);
    /* refusals leave dst unchanged */
    CHECK_RC(moq_locmaf_decoder_copy(&live.d, &live.d), MOQ_ERR_INVAL);
    CHECK_RC(moq_locmaf_decoder_copy(NULL, &live.d), MOQ_ERR_INVAL);
    CHECK_RC(moq_locmaf_decoder_copy(&live.d, NULL), MOQ_ERR_INVAL);
    {
        moq_locmaf_track_t ta = lm_track_a();
        lm_dec_t other;
        open_dec(&other, &ta);
        CHECK_RC(moq_locmaf_decoder_copy(&other.d, &fill.d), MOQ_ERR_INVAL);
        CHECK(!moq_locmaf_decoder_expects(&other.d, 5, 4));
        lm_dec_close(&other);
    }
    {
        /* dst limits below the lists src's reference holds */
        moq_locmaf_track_t ta = lm_track_a();
        lm_dec_t big, small;
        open_dec(&big, &ta);
        CHECK_RC(lm_dec_open(&small, &ta, 2, 0, 0), MOQ_OK);
        CHECK_RC(push_hex(&big, 0, 0, FULL, "01 02 06 05 0A 88 00 0E 03", 15, &ch, &why),
                 MOQ_OK);
        CHECK_RC(moq_locmaf_decoder_copy(&small.d, &big.d), MOQ_ERR_INVAL);
        CHECK(!moq_locmaf_decoder_expects(&small.d, 0, 1));
        lm_dec_close(&big);
        lm_dec_close(&small);
    }
    lm_dec_close(&fill);
    lm_dec_close(&live);
}

/* -- Unknown IDs, delta rules -------------------------------------------------- */

static void test_unknown_and_delta(void)
{
    moq_locmaf_track_t ta = lm_track_a();
    moq_locmaf_chunk_t ch;
    moq_locmaf_reason_t why;
    lm_dec_t dec;

    /* unknown_even_odd_skipped */
    open_dec(&dec, &ta);
    CHECK_RC(push_hex(&dec, 0, 0, FULL, "0A 00 12 05 13 02 AABB 0E 01", 4, &ch, &why),
             MOQ_OK);
    CHECK(ch.sample_count == 1 && ch.samples[0].size == 4 &&
          ch.samples[0].duration == 1024);
    CHECK_EQ(stats(&dec).unknown_fields_skipped, 2);
    /* unknown IDs are never in the reference: deleting one is refused */
    CHECK_RC(push_hex(&dec, 0, 1, DELTA, "1B 01 12", 4, &ch, &why), MOQ_ERR_PROTO);
    CHECK_WHY(why, MOQ_LOCMAF_REASON_DELETION);
    lm_dec_close(&dec);

    /* unknown_dup_rejected, decode_dup_known */
    open_dec(&dec, &ta);
    CHECK_RC(push_hex(&dec, 0, 0, FULL, "12 05 0A 00 12 06 0E 01", 4, &ch, &why),
             MOQ_ERR_PROTO);
    CHECK_WHY(why, MOQ_LOCMAF_REASON_DUPLICATE_FIELD);
    CHECK_RC(push_hex(&dec, 0, 1, FULL, "13 00 0A 00 13 01 00 0E 01", 4, &ch, &why),
             MOQ_ERR_PROTO);
    CHECK_WHY(why, MOQ_LOCMAF_REASON_DUPLICATE_FIELD);
    CHECK_RC(push_hex(&dec, 0, 2, FULL, "0A 00 0A 00 0E 01", 4, &ch, &why), MOQ_ERR_PROTO);
    CHECK_WHY(why, MOQ_LOCMAF_REASON_DUPLICATE_FIELD);
    lm_dec_close(&dec);

    /* unknown_65_limit: 64 distinct unknown IDs pass, 65 do not */
    open_dec(&dec, &ta);
    for (unsigned count = 64; count <= 65; count++) {
        uint8_t pb[512];
        bld_t p = { pb, 0, sizeof(pb) };
        b_scalar(&p, 10, 0);
        b_scalar(&p, 14, 1);
        for (unsigned k = 0; k < count; k++) b_scalar(&p, 18 + 2 * k, 0);
        moq_result_t rc = push_bld(&dec, count, 0, FULL, &p, 4, &ch, &why);
        CHECK_RC(rc, count == 64 ? MOQ_OK : MOQ_ERR_LIMIT);
        CHECK_WHY(why, count == 64 ? MOQ_LOCMAF_REASON_NONE : MOQ_LOCMAF_REASON_LIMIT);
    }
    lm_dec_close(&dec);

    /* scalar introduction from absent: previous value 0 */
    open_dec(&dec, &ta);
    CHECK_RC(push_hex(&dec, 0, 0, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_OK);
    CHECK_RC(push_hex(&dec, 0, 1, DELTA, "04 8F A0", 4, &ch, &why), MOQ_OK);  /* zz(2000) */
    CHECK_EQ(ch.samples[0].duration, 2000);
    CHECK_EQ(ch.base_media_decode_time, 1024);
    CHECK_RC(push_hex(&dec, 0, 2, DELTA, "", 4, &ch, &why), MOQ_OK);
    CHECK_EQ(ch.samples[0].duration, 2000);
    CHECK_EQ(ch.base_media_decode_time, 3024);
    /* deleting it falls back to trex */
    CHECK_RC(push_hex(&dec, 0, 3, DELTA, "1B 01 04", 4, &ch, &why), MOQ_OK);
    CHECK_EQ(ch.samples[0].duration, 1024);
    CHECK_EQ(ch.base_media_decode_time, 5024);
    lm_dec_close(&dec);

    /* delete-then-re-add in one header, and the other deletion refusals */
    {
        static const char *bad[] = {
            "08 00 1B 01 08",   /* carried and deleted */
            "1B 01 02",         /* absent */
            "1B 01 0A",         /* 10 */
            "1B 01 0E",         /* 14 */
            "1B 01 1B",         /* 27 */
            "1B 01 12",         /* unknown */
            "1B 02 08 08",      /* twice */
            "1B 01 C0",         /* truncated element */
        };
        moq_locmaf_track_t tv = lm_track_v();
        for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
            open_dec(&dec, &tv);
            CHECK_RC(push_hex(&dec, 0, 0, FULL, "04 8B B8 08 E2 00 00 00 0A CD BB A0 0E 01",
                              5, &ch, &why), MOQ_OK);
            CHECK_RC(push_hex(&dec, 0, 1, DELTA, bad[i], 7, &ch, &why), MOQ_ERR_PROTO);
            CHECK_WHY(why, i == 7 ? MOQ_LOCMAF_REASON_TRUNCATED : MOQ_LOCMAF_REASON_DELETION);
            /* the refusal discarded the reference */
            CHECK_RC(push_hex(&dec, 0, 2, DELTA, "", 7, &ch, &why), MOQ_ERR_WRONG_STATE);
            lm_dec_close(&dec);
        }
    }

    /* inherited lists: truncated when n shrinks, refused when it grows */
    open_dec(&dec, &ta);
    CHECK_RC(push_hex(&dec, 0, 0, FULL, "01 02 06 05 0A 88 00 0E 03", 15, &ch, &why), MOQ_OK);
    CHECK_RC(push_hex(&dec, 0, 1, DELTA, "0E 01", 9, &ch, &why), MOQ_OK);   /* n 3 -> 2 */
    CHECK_EQ(ch.sample_count, 2);
    CHECK(ch.samples[0].size == 6 && ch.samples[1].size == 3);
    CHECK_RC(push_hex(&dec, 0, 2, DELTA, "0E 02", 9, &ch, &why), MOQ_ERR_PROTO);  /* 2 -> 3 */
    CHECK_WHY(why, MOQ_LOCMAF_REASON_LIST_LENGTH);
    lm_dec_close(&dec);
}

/* -- Refusals ---------------------------------------------------------------------- */

static void expect_full(const moq_locmaf_track_t *t, const char *props, size_t payload,
                        moq_result_t rc, moq_locmaf_reason_t r)
{
    lm_dec_t dec;
    open_dec(&dec, t);
    moq_locmaf_chunk_t ch;
    moq_locmaf_reason_t why;
    memset(&ch, 0xEE, sizeof(ch));
    moq_result_t got = push_hex(&dec, 0, 0, FULL, props, payload, &ch, &why);
    if (got != rc || why != r) fprintf(stderr, "  full header: %s (+%zu)\n", props, payload);
    CHECK_RC(got, rc);
    CHECK_WHY(why, r);
    if (rc != MOQ_OK) {
        CHECK(ch.struct_size == 0xEEEEEEEEu);
        CHECK(!moq_locmaf_decoder_expects(&dec.d, 0, 1));
    }
    lm_dec_close(&dec);
}

static void expect_delta(const moq_locmaf_track_t *t, const char *full, size_t p0,
                         const char *delta, size_t p1, moq_result_t rc,
                         moq_locmaf_reason_t r)
{
    lm_dec_t dec;
    open_dec(&dec, t);
    moq_locmaf_chunk_t ch;
    moq_locmaf_reason_t why;
    CHECK_RC(push_hex(&dec, 0, 0, FULL, full, p0, &ch, &why), MOQ_OK);
    moq_result_t got = push_hex(&dec, 0, 1, DELTA, delta, p1, &ch, &why);
    if (got != rc || why != r) fprintf(stderr, "  delta: %s after %s\n", delta, full);
    CHECK_RC(got, rc);
    CHECK_WHY(why, r);
    lm_dec_close(&dec);
}

static void test_refusals(void)
{
    moq_locmaf_track_t ta = lm_track_a();
    moq_locmaf_track_t t4 = lm_track_a();
    t4.trex_default_sample_size = 4;
    const moq_result_t P = MOQ_ERR_PROTO;

    /* section 12.2, 10.4: kinds */
    expect_delta(&ta, "0A 00 0E 01", 4, "0A 00", 4, P, MOQ_LOCMAF_REASON_FIELD_KIND);
    expect_full(&ta, "1B 01 08 0A 00 0E 01", 4, P, MOQ_LOCMAF_REASON_FIELD_KIND);
    /* missing required fields */
    expect_full(&ta, "0A 00", 4, P, MOQ_LOCMAF_REASON_MISSING_FIELD);
    expect_full(&ta, "0E 01", 4, P, MOQ_LOCMAF_REASON_MISSING_FIELD);
    /* sample sizes (section 11.2) */
    expect_full(&ta, "0A 00 0E 02", 10, P, MOQ_LOCMAF_REASON_SAMPLE_SIZES);
    expect_full(&ta, "01 01 14 0A 00 0E 02", 10, P, MOQ_LOCMAF_REASON_SAMPLE_SIZES);
    expect_full(&ta, "06 04 0A 00 0E 02", 10, P, MOQ_LOCMAF_REASON_SAMPLE_SIZES);
    expect_full(&t4, "0A 00 0E 02", 10, P, MOQ_LOCMAF_REASON_SAMPLE_SIZES);
    expect_full(&t4, "0A 00 0E 02", 8, MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    expect_full(&ta, "0A 00 0E 00", 3, P, MOQ_LOCMAF_REASON_SAMPLE_SIZES);
    expect_full(&ta, "0A 00 0E 02", 0, MOQ_OK, MOQ_LOCMAF_REASON_NONE);   /* P == 0 */
    expect_full(&ta, "01 01 0A 0A 00 0E 02", 10, MOQ_OK, MOQ_LOCMAF_REASON_NONE); /* 10, 0 */
    expect_full(&ta, "06 05 0A 00 0E 02", 10, MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    /* ID 1 is consulted alone: a contradicting ID 6 is ignored */
    expect_full(&ta, "01 01 03 06 09 0A 00 0E 02", 10, MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    /* list lengths (n = 3) */
    expect_full(&ta, "01 01 02 0A 00 0E 03", 9, P, MOQ_LOCMAF_REASON_LIST_LENGTH);
    expect_full(&ta, "01 03 02 02 02 0A 00 0E 03", 9, P, MOQ_LOCMAF_REASON_LIST_LENGTH);
    expect_full(&ta, "03 02 01 01 0A 00 0E 03", 0, P, MOQ_LOCMAF_REASON_LIST_LENGTH);
    expect_full(&ta, "05 04 00 00 00 00 0A 00 0E 03", 0, P, MOQ_LOCMAF_REASON_LIST_LENGTH);
    expect_full(&ta, "07 02 00 00 0A 00 0E 03", 0, P, MOQ_LOCMAF_REASON_LIST_LENGTH);
    expect_full(&ta, "01 01 C0 0A 00 0E 02", 10, P, MOQ_LOCMAF_REASON_TRUNCATED);
    /* ID 1 at n = 0 holds nothing */
    expect_full(&ta, "01 01 00 0A 00 0E 00", 0, P, MOQ_LOCMAF_REASON_LIST_LENGTH);
    expect_full(&ta, "01 00 0A 00 0E 00", 0, MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    /* values outside their ISO fields */
    expect_full(&ta, "08 F1 00 00 00 00 0A 00 0E 01", 4, P, MOQ_LOCMAF_REASON_VALUE_RANGE);
    expect_full(&ta, "08 F0 FF FF FF FF 0A 00 0E 01", 4, MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    expect_full(&ta, "02 F1 00 00 00 00 0A 00 0E 01", 4, P, MOQ_LOCMAF_REASON_VALUE_RANGE);
    expect_full(&ta, "03 05 F1 00 00 00 00 0A 00 0E 01", 4, P, MOQ_LOCMAF_REASON_VALUE_RANGE);
    expect_full(&ta, "0E F1 00 00 00 00 0A 00", 0, P, MOQ_LOCMAF_REASON_VALUE_RANGE);
    expect_delta(&ta, "04 8B B8 0A 00 0E 01", 4, "04 97 71", 4, P,  /* zz(-3001) */
                 MOQ_LOCMAF_REASON_VALUE_RANGE);
    /* arith_cto_joint: no trun version holds both -1 and 2^31 */
    expect_full(&ta, "05 06 01 F1 00 00 00 00 0A 00 0E 02", 0, P,
                MOQ_LOCMAF_REASON_VALUE_RANGE);
    /* limits */
    {
        lm_dec_t dec;
        CHECK_RC(lm_dec_open(&dec, &ta, 4, 0, 2), MOQ_OK);
        moq_locmaf_chunk_t ch;
        moq_locmaf_reason_t why;
        CHECK_RC(push_hex(&dec, 0, 0, FULL, "0A 00 0E 05", 0, &ch, &why), MOQ_ERR_LIMIT);
        CHECK_WHY(why, MOQ_LOCMAF_REASON_LIMIT);
        CHECK_RC(push_hex(&dec, 0, 1, FULL, "0A 00 0E 04", 0, &ch, &why), MOQ_OK);
        /* genBoxes beyond max_gen_boxes */
        uint8_t obj[64];
        size_t n = lm_hex("01 04 66726565 01 04 66726565 01 04 66726565 02 04 0A 00 0E 00",
                          obj, sizeof(obj));
        CHECK_RC(moq_locmaf_decoder_push(&dec.d, 1, 0, (moq_bytes_t){ obj, n }, &ch,
                                         sizeof(ch), &why), MOQ_ERR_LIMIT);
        CHECK_WHY(why, MOQ_LOCMAF_REASON_LIMIT);
        n = lm_hex("01 04 66726565 01 04 66726565 02 04 0A 00 0E 00", obj, sizeof(obj));
        CHECK_RC(moq_locmaf_decoder_push(&dec.d, 1, 1, (moq_bytes_t){ obj, n }, &ch,
                                         sizeof(ch), &why), MOQ_OK);
        CHECK_EQ(ch.gen_box_count, 2);
        lm_dec_close(&dec);
    }
    /* object-level malformations reach the decoder with their reasons */
    expect_full(&ta, "0A 00 0E 01 13 05 00", 4, P, MOQ_LOCMAF_REASON_PROPERTY_FRAMING);
    {
        lm_dec_t dec;
        open_dec(&dec, &ta);
        moq_locmaf_chunk_t ch;
        moq_locmaf_reason_t why;
        uint8_t obj[16];
        size_t n = lm_hex("01 04 61626364 02 04 0A 00 0E 00", obj, sizeof(obj));
        CHECK_RC(moq_locmaf_decoder_push(&dec.d, 0, 0, (moq_bytes_t){ obj, n }, &ch,
                                         sizeof(ch), &why), MOQ_ERR_UNSUPPORTED);
        CHECK_WHY(why, MOQ_LOCMAF_REASON_GEN_BOX_UNSUPPORTED);
        CHECK_RC(moq_locmaf_decoder_push(&dec.d, 0, 1, (moq_bytes_t){ obj, 0 }, &ch,
                                         sizeof(ch), &why), MOQ_ERR_PROTO);
        CHECK_WHY(why, MOQ_LOCMAF_REASON_EMPTY_OBJECT);
        lm_dec_close(&dec);
    }
}

/* -- Checked arithmetic --------------------------------------------------------- */

static void test_arith(void)
{
    moq_locmaf_track_t ta = lm_track_a();
    const moq_result_t P = MOQ_ERR_PROTO;
    const moq_locmaf_reason_t R = MOQ_LOCMAF_REASON_VALUE_RANGE;
    char full[128], delta[128];

    /* arith_scalar_min / _max: the extreme deltas never wrap */
    expect_delta(&ta, "04 00 0A 00 0E 01", 4, "04 FF FF FF FF FF FF FF FF FF", 4, P, R);
    expect_delta(&ta, "04 F0 FF FF FF FF 0A 00 0E 01", 4,
                 "04 FF FF FF FF FF FF FF FF FE", 4, P, R);
    /* arith_scalar_edges */
    expect_delta(&ta, "04 05 0A 00 0E 01", 4, "04 09", 4, MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    expect_delta(&ta, "04 05 0A 00 0E 01", 4, "04 0B", 4, P, R);
    expect_delta(&ta, "04 F0 FF FF FF FE 0A 00 0E 01", 4, "04 02", 4, MOQ_OK,
                 MOQ_LOCMAF_REASON_NONE);
    expect_delta(&ta, "04 F0 FF FF FF FE 0A 00 0E 01", 4, "04 04", 4, P, R);
    /* arith_cto_edges */
    snprintf(full, sizeof(full), "05 05 F0 FF FF FF FF 0A 00 0E 01");   /* -2^31 */
    expect_delta(&ta, full, 4, "05 01 01", 4, P, R);                     /* d = -1 */
    snprintf(full, sizeof(full), "05 05 F1 FF FF FF FE 0A 00 0E 01");   /* 2^32-1 */
    expect_delta(&ta, full, 4, "05 01 02", 4, P, R);                     /* d = +1 */
    expect_delta(&ta, "05 01 00 0A 00 0E 01", 4, "05 05 F1 FF FF FF FE", 4, MOQ_OK,
                 MOQ_LOCMAF_REASON_NONE);                                /* 0 + 2^32-1 */
    snprintf(full, sizeof(full), "05 05 F0 FF FF FF FF 0A 00 0E 01");
    snprintf(delta, sizeof(delta), "05 05 F2 FF FF FF FE");             /* 2^32-1+2^31 */
    expect_delta(&ta, full, 4, delta, 4, MOQ_OK, MOQ_LOCMAF_REASON_NONE);
    /* arith_cto_minmax_delta */
    expect_delta(&ta, "05 01 00 0A 00 0E 01", 4, "05 09 FF FF FF FF FF FF FF FF FF", 4, P, R);
    expect_delta(&ta, "05 01 00 0A 00 0E 01", 4, "05 09 FF FF FF FF FF FF FF FF FE", 4, P, R);
    /* arith_count_max: the range check comes before the limit */
    expect_delta(&ta, "0A 00 0E 01", 4, "0E FF FF FF FF FF FF FF FF FE", 4, P, R);
    /* arith_list_growth: a new element's absolute value INT64_MIN */
    expect_delta(&ta, "01 01 05 0A 00 0E 02", 10, "01 0A 00 FF FF FF FF FF FF FF FF FF 0E 02",
                 10, P, R);
    /* arith_bmdt_full_max: a full UINT64_MAX is fine; its continuation is not */
    expect_full(&ta, "04 01 0A FF FF FF FF FF FF FF FF FF 0E 01", 4, MOQ_OK,
                MOQ_LOCMAF_REASON_NONE);
    expect_delta(&ta, "04 01 0A FF FF FF FF FF FF FF FF FF 0E 01", 4, "", 4, P, R);
    expect_delta(&ta, "04 01 0A FF FF FF FF FF FF FF FF FE 0E 01", 4, "", 4, MOQ_OK,
                 MOQ_LOCMAF_REASON_NONE);

    /* arith_duration_sum: 65536 durations of 2^32-1, summed exactly */
    {
        lm_dec_t dec;
        CHECK_RC(lm_dec_open(&dec, &ta, 65536, 0, 0), MOQ_OK);
        bld_t p = b_new(65536 * 5 + 64);
        uint64_t *durs = malloc(65536 * sizeof(uint64_t));
        CHECK(durs != NULL);
        if (durs) {
            for (size_t i = 0; i < 65536; i++) durs[i] = 0xFFFFFFFFu;
            b_list(&p, 3, durs, 65536);
            b_scalar(&p, 10, 0);
            b_scalar(&p, 14, 65536);
            moq_locmaf_chunk_t ch;
            moq_locmaf_reason_t why;
            CHECK_RC(push_bld(&dec, 0, 0, FULL, &p, 0, &ch, &why), MOQ_OK);
            CHECK_EQ(ch.sample_count, 65536);
            CHECK_EQ(ch.samples[65535].duration, 0xFFFFFFFFu);
            bld_t e = b_new(16);
            CHECK_RC(push_bld(&dec, 0, 1, DELTA, &e, 0, &ch, &why), MOQ_OK);
            CHECK_EQ(ch.base_media_decode_time, 65536ull * 0xFFFFFFFFull);
            free(e.b);
        }
        free(durs);
        free(p.b);
        lm_dec_close(&dec);
    }
}

/* -- Counters ------------------------------------------------------------------- */

static void test_stats(void)
{
    moq_locmaf_track_t ta = lm_track_a();
    moq_locmaf_chunk_t ch;
    moq_locmaf_reason_t why;
    lm_dec_t dec;
    CHECK_RC(lm_dec_open(&dec, &ta, 4, 0, 0), MOQ_OK);
    moq_locmaf_decoder_stats_t s0 = stats(&dec), s;
    CHECK_EQ(s0.struct_size, MOQ_LOCMAF_DECODER_STATS_V0_SIZE);

#define DELTA_STAT(field, k) CHECK_EQ(s.field - s0.field, (k))
#define STEP() do { s0 = s; s = stats(&dec); } while (0)
    s = s0;
    CHECK_RC(push_hex(&dec, 0, 0, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_OK);
    STEP();
    DELTA_STAT(objects_decoded, 1); DELTA_STAT(full_headers_decoded, 1);
    DELTA_STAT(references_discarded, 0);
    CHECK_RC(push_hex(&dec, 0, 1, DELTA, "", 4, &ch, &why), MOQ_OK);
    STEP();
    DELTA_STAT(objects_decoded, 1); DELTA_STAT(delta_headers_decoded, 1);
    /* a gap with a reference held */
    CHECK_RC(push_hex(&dec, 0, 3, DELTA, "", 4, &ch, &why), MOQ_ERR_WRONG_STATE);
    STEP();
    DELTA_STAT(objects_unreferenced, 1); DELTA_STAT(references_discarded, 1);
    CHECK_WHY(s.last_reason, MOQ_LOCMAF_REASON_NO_REFERENCE);
    /* a new group with a reference held */
    CHECK_RC(push_hex(&dec, 0, 4, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_OK);
    CHECK_RC(push_hex(&dec, 1, 0, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_OK);
    STEP();
    DELTA_STAT(objects_decoded, 2); DELTA_STAT(references_discarded, 1);
    /* a reset with a reference held; reset keeps the counters */
    moq_locmaf_decoder_reset(&dec.d);
    STEP();
    DELTA_STAT(references_discarded, 1);
    CHECK_EQ(s.objects_decoded, 4);
    /* rawBoxes with a reference held */
    {
        uint8_t raw[256];
        raw[0] = 4;
        size_t cl = lm_fixture_canonical(&lm_sequences[0].items[0], raw + 1, sizeof(raw) - 1);
        CHECK_RC(push_hex(&dec, 2, 0, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_OK);
        CHECK_RC(moq_locmaf_decoder_push(&dec.d, 2, 1, (moq_bytes_t){ raw, cl + 1 }, &ch,
                                         sizeof(ch), &why), MOQ_OK);
        STEP();
        DELTA_STAT(objects_decoded, 2); DELTA_STAT(raw_boxes_decoded, 1);
        DELTA_STAT(full_headers_decoded, 1); DELTA_STAT(references_discarded, 1);
    }
    /* a refusal with a reference held */
    CHECK_RC(push_hex(&dec, 3, 0, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_OK);
    CHECK_RC(push_hex(&dec, 3, 1, DELTA, "0A 00", 4, &ch, &why), MOQ_ERR_PROTO);
    STEP();
    DELTA_STAT(objects_malformed, 1); DELTA_STAT(references_discarded, 1);
    CHECK_WHY(s.last_reason, MOQ_LOCMAF_REASON_FIELD_KIND);
    /* a refusal with none held moves no discard */
    CHECK_RC(push_hex(&dec, 3, 2, FULL, "0A 00 0E 09", 0, &ch, &why), MOQ_ERR_LIMIT);
    STEP();
    DELTA_STAT(objects_limited, 1); DELTA_STAT(references_discarded, 0);
    CHECK_WHY(s.last_reason, MOQ_LOCMAF_REASON_LIMIT);
    {
        uint8_t obj[16];
        size_t n = lm_hex("01 04 61626364 02 04 0A 00 0E 00", obj, sizeof(obj));
        CHECK_RC(moq_locmaf_decoder_push(&dec.d, 3, 3, (moq_bytes_t){ obj, n }, &ch,
                                         sizeof(ch), &why), MOQ_ERR_UNSUPPORTED);
        STEP();
        DELTA_STAT(objects_unsupported, 1);
        CHECK_WHY(s.last_reason, MOQ_LOCMAF_REASON_GEN_BOX_UNSUPPORTED);
    }
    /* INVAL moves nothing */
    CHECK_RC(push_hex(&dec, 3, 0, FULL, "0A 00 0E 01", 4, &ch, &why), MOQ_ERR_INVAL);
    STEP();
    CHECK(memcmp(&s, &s0, sizeof(s)) == 0);
    CHECK_EQ(s.objects_decoded, s.full_headers_decoded + s.delta_headers_decoded +
                                s.raw_boxes_decoded);
#undef DELTA_STAT
#undef STEP
    lm_dec_close(&dec);
}

/* -- Track facts from a CMAF Header --------------------------------------------- */

static void test_track_from_cmaf(void)
{
    static const uint32_t trex4[4] = { 1, 1024, 0, 0x02000000u };
    uint8_t buf[1024];
    moq_cmaf_init_info_t init;
    moq_locmaf_track_t t;

    size_t len = moq_test_build_cmaf_init_trex(buf, 3, 48000, trex4, 1, true);
    CHECK_RC(moq_cmaf_parse_init_sized((moq_bytes_t){ buf, len }, &init, sizeof(init)), MOQ_OK);
    memset(&t, 0xEE, sizeof(t));
    CHECK_RC(moq_locmaf_track_from_cmaf(&init, &t, sizeof(t)), MOQ_OK);
    CHECK_EQ(t.struct_size, sizeof(t));
    CHECK(t.track_id == 3 && t.trex_default_sample_description_index == 1 &&
          t.trex_default_sample_duration == 1024 && t.trex_default_sample_size == 0 &&
          t.trex_default_sample_flags == 0x02000000u && !t.is_protected &&
          t.default_per_sample_iv_size == 0 && t._reserved[0] == 0 && t._reserved[1] == 0);
    /* old_init_stamp: a record stamped by a library without the facts */
    moq_cmaf_init_info_t old;
    CHECK_RC(moq_cmaf_parse_init_sized((moq_bytes_t){ buf, len }, &old,
             MOQ_CMAF_INIT_INFO_SIZE_THROUGH_RESERVED), MOQ_OK);
    CHECK_RC(moq_locmaf_track_from_cmaf(&old, &t, sizeof(t)), MOQ_ERR_INVAL);
    /* trak 2, no trex, track_id 0 */
    len = moq_test_build_cmaf_init_trex(buf, 3, 48000, trex4, 2, true);
    CHECK_RC(moq_cmaf_parse_init_sized((moq_bytes_t){ buf, len }, &init, sizeof(init)), MOQ_OK);
    CHECK_RC(moq_locmaf_track_from_cmaf(&init, &t, sizeof(t)), MOQ_ERR_PROTO);
    len = moq_test_build_cmaf_init_trex(buf, 3, 48000, trex4, 1, false);
    CHECK_RC(moq_cmaf_parse_init_sized((moq_bytes_t){ buf, len }, &init, sizeof(init)), MOQ_OK);
    CHECK_RC(moq_locmaf_track_from_cmaf(&init, &t, sizeof(t)), MOQ_ERR_PROTO);
    len = moq_test_build_cmaf_init_trex(buf, 0, 48000, trex4, 1, true);
    CHECK_RC(moq_cmaf_parse_init_sized((moq_bytes_t){ buf, len }, &init, sizeof(init)), MOQ_OK);
    CHECK_RC(moq_locmaf_track_from_cmaf(&init, &t, sizeof(t)), MOQ_ERR_PROTO);
    /* protection: the tenc defaults; an IV size a track cannot have */
    {
        static const uint8_t kid[16] = { 1 };
        static const uint8_t avcc[] = { 0x01, 0x64, 0x00, 0x1E, 0xFF };
        moq_test_cenc_init_opts_t o = { .with_trex = true,
                                        .trex = { 1, 3000, 0, 0x01010000u } };
        len = moq_test_build_cmaf_init_cenc_ex(buf, 1, 90000, 64, 64, avcc, sizeof(avcc),
                                               "cenc", 1, 8, kid, 0, MOQ_TEST_CENC_OK, &o);
        CHECK_RC(moq_cmaf_parse_init_sized((moq_bytes_t){ buf, len }, &init, sizeof(init)),
                 MOQ_OK);
        CHECK_RC(moq_locmaf_track_from_cmaf(&init, &t, sizeof(t)), MOQ_OK);
        CHECK(t.is_protected && t.default_per_sample_iv_size == 8 &&
              t.trex_default_sample_duration == 3000);
        len = moq_test_build_cmaf_init_cenc_ex(buf, 1, 90000, 64, 64, avcc, sizeof(avcc),
                                               "cenc", 1, 12, kid, 0, MOQ_TEST_CENC_OK, &o);
        CHECK_RC(moq_cmaf_parse_init_sized((moq_bytes_t){ buf, len }, &init, sizeof(init)),
                 MOQ_OK);
        CHECK_RC(moq_locmaf_track_from_cmaf(&init, &t, sizeof(t)), MOQ_ERR_PROTO);
        /* default_isProtected 0: a clear track, whatever its IV size says */
        len = moq_test_build_cmaf_init_cenc_ex(buf, 1, 90000, 64, 64, avcc, sizeof(avcc),
                                               "cenc", 0, 8, kid, 0, MOQ_TEST_CENC_OK, &o);
        CHECK_RC(moq_cmaf_parse_init_sized((moq_bytes_t){ buf, len }, &init, sizeof(init)),
                 MOQ_OK);
        CHECK_RC(moq_locmaf_track_from_cmaf(&init, &t, sizeof(t)), MOQ_OK);
        CHECK(!t.is_protected && t.default_per_sample_iv_size == 0);
    }
    CHECK_RC(moq_locmaf_track_from_cmaf(NULL, &t, sizeof(t)), MOQ_ERR_INVAL);
    CHECK_RC(moq_locmaf_track_from_cmaf(&init, NULL, sizeof(t)), MOQ_ERR_INVAL);
}

int main(void)
{
    test_fixtures();
    test_sequencing();
    test_copy();
    test_unknown_and_delta();
    test_refusals();
    test_arith();
    test_stats();
    test_track_from_cmaf();
    return lm_finish("locmaf_decode");
}
