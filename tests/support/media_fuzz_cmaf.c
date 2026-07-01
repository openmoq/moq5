/*
 * Seeded CMAF object-validation property test (generate-only). See media_fuzz.h.
 *
 * Generates structurally valid and invalid CMAF objects together with their
 * EXACT expected moq_cmaf_object_report_t outcome, then asserts
 * moq_cmaf_validate_object() reproduces it precisely -- not merely "failed", but
 * the specific first-failure reason the validator contract defines (the order:
 * not-moof / read error -> MALFORMED; missing mfhd -> MISSING_MFHD; missing or
 * second traf / bad tfhd -> MALFORMED|MULTI_TRACK; mdat phase: not-mdat ->
 * MALFORMED, unparseable/zero samples -> NO_SAMPLES; post-loop: trailing or
 * dangling moof -> MALFORMED, no chunk -> NO_CHUNK, init track_id mismatch ->
 * TRACK_ID_MISMATCH). Valid objects also assert parse_fragment normalization
 * (first chunk's track_id/mdat) and that mdat borrows from the input buffer.
 *
 * Objects are composed from the shared media_builders chunk builder (moof+mdat)
 * plus its box-header helper; ordering cases slice the built chunk into its moof
 * and mdat boxes and recompose them. This is pure media/cmaf validation -- it
 * does NOT touch the service sender's strict-validation layer.
 *
 * No allocator: moq_cmaf_validate_object / parse_fragment are stack-only, so
 * there is no per-case heap to leak-balance (unlike the loc/msf runs).
 */

#include "media_fuzz.h"
#include "media_builders.h"

#include <moq/types.h>
#include <moq/cmaf.h>

#include "../unit/test_support.h"

#include <stdio.h>
#include <string.h>

/* Sample-flag presets and the SAP type each implies (computed independently of
 * moq_cmaf_sap_from_sample_flags so the assertion is not self-referential). */
#define FLAGS_SYNC        0x02000000u  /* sync sample      -> SAP_UNKNOWN */
#define FLAGS_NONSYNC_P   0x01010000u  /* non-sync deps=1  -> SAP_NONE    */
#define FLAGS_NONSYNC_OPEN 0x02010000u /* non-sync deps=2  -> SAP_UNKNOWN */

typedef enum {
    K_VALID_SYNC = 0,
    K_VALID_NONSYNC_P,
    K_VALID_NONSYNC_OPEN,
    K_VALID_INIT_MATCH,
    K_VALID_MULTI,
    K_MDAT_FIRST,
    K_MOOF_MOOF,
    K_DANGLING_MOOF,
    K_STRAY_BOX,
    K_MISSING_MFHD,
    K_NO_SAMPLES,
    K_MULTI_TRAF,
    K_MULTI_TRACK_CHUNKS,
    K_TRACK_ID_MISMATCH,
    K_NO_CHUNK,
    K_TRUNCATED_BOX,
    K_COUNT
} cmaf_kind_t;

typedef struct {
    const char         *name;
    uint8_t             buf[4096];
    size_t              len;
    bool                use_init;
    uint32_t            init_track_id;
    bool                exp_valid;
    moq_cmaf_validity_t exp_reason;
    uint32_t            exp_chunk_count;
    uint32_t            exp_track_id;
    bool                check_sap;
    moq_sap_type_t      exp_sap;
    uint8_t             first_mdat_byte;   /* valid cases: first chunk mdat byte */
} cmaf_case_t;

static uint32_t rd32be(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8)  | (uint32_t)p[3];
}

static moq_sap_type_t sap_for(uint32_t flags)
{
    if (flags == FLAGS_SYNC)         return MOQ_SAP_UNKNOWN;
    if (flags == FLAGS_NONSYNC_P)    return MOQ_SAP_NONE;
    if (flags == FLAGS_NONSYNC_OPEN) return MOQ_SAP_UNKNOWN;
    return MOQ_SAP_UNKNOWN;
}

/* Append a built chunk (moof+mdat) to c->buf; returns its length. */
static size_t append_chunk(cmaf_case_t *c, uint32_t tid, uint32_t flags,
                           bool include_mfhd, bool two_traf, uint8_t mdat_byte)
{
    size_t n = moq_test_build_cmaf_chunk(c->buf + c->len, tid, flags,
                                         include_mfhd, two_traf, mdat_byte);
    c->len += n;
    return n;
}

static void gen_case(moq_fuzz_rng_t *rng, cmaf_case_t *c)
{
    memset(c, 0, sizeof(*c));
    c->exp_reason = MOQ_CMAF_OK;
    c->exp_sap = MOQ_SAP_UNKNOWN;

    cmaf_kind_t kind = (cmaf_kind_t)moq_fuzz_rng_below(rng, K_COUNT);
    uint32_t tid = 1 + (uint32_t)moq_fuzz_rng_below(rng, 64);
    uint8_t mb = (uint8_t)moq_fuzz_rng_below(rng, 256);
    const uint32_t flag_set[] = { FLAGS_SYNC, FLAGS_NONSYNC_P, FLAGS_NONSYNC_OPEN };

    switch (kind) {
    case K_VALID_SYNC:
    case K_VALID_NONSYNC_P:
    case K_VALID_NONSYNC_OPEN: {
        uint32_t flags = (kind == K_VALID_SYNC) ? FLAGS_SYNC
            : (kind == K_VALID_NONSYNC_P) ? FLAGS_NONSYNC_P : FLAGS_NONSYNC_OPEN;
        c->name = "valid-single";
        append_chunk(c, tid, flags, true, false, mb);
        c->exp_valid = true; c->exp_chunk_count = 1; c->exp_track_id = tid;
        c->check_sap = true; c->exp_sap = sap_for(flags);
        c->first_mdat_byte = mb;
        break;
    }
    case K_VALID_INIT_MATCH: {
        uint32_t flags = flag_set[moq_fuzz_rng_below(rng, 3)];
        c->name = "valid-init-match";
        append_chunk(c, tid, flags, true, false, mb);
        c->use_init = true; c->init_track_id = tid;
        c->exp_valid = true; c->exp_chunk_count = 1; c->exp_track_id = tid;
        c->check_sap = true; c->exp_sap = sap_for(flags);
        c->first_mdat_byte = mb;
        break;
    }
    case K_VALID_MULTI: {
        uint32_t k = 2 + (uint32_t)moq_fuzz_rng_below(rng, 3);   /* 2..4 chunks */
        uint32_t flags0 = flag_set[moq_fuzz_rng_below(rng, 3)];
        c->name = "valid-multi";
        append_chunk(c, tid, flags0, true, false, mb);          /* chunk 0 */
        for (uint32_t i = 1; i < k; i++)
            append_chunk(c, tid, FLAGS_SYNC, true, false,
                         (uint8_t)moq_fuzz_rng_below(rng, 256));
        c->exp_valid = true; c->exp_chunk_count = k; c->exp_track_id = tid;
        c->check_sap = true; c->exp_sap = sap_for(flags0);
        c->first_mdat_byte = mb;
        break;
    }
    case K_MDAT_FIRST: {
        /* a lone mdat box where a moof is expected */
        c->name = "invalid-mdat-first";
        c->len += moq_test_box_hdr(c->buf + c->len, 9, "mdat");
        c->buf[c->len++] = mb;
        c->exp_valid = false; c->exp_reason = MOQ_CMAF_ERR_MALFORMED;
        c->exp_chunk_count = 0; c->exp_track_id = 0;
        break;
    }
    case K_MOOF_MOOF: {
        /* moof | moof | mdat: the second moof arrives where mdat is expected.
         * The first moof is fully processed (tfhd read), so track_id is set. */
        uint8_t tmp[512];
        size_t total = moq_test_build_cmaf_chunk(tmp, tid, FLAGS_SYNC, true, false, mb);
        size_t moof_sz = rd32be(tmp);
        memcpy(c->buf + c->len, tmp, moof_sz); c->len += moof_sz;          /* moof */
        memcpy(c->buf + c->len, tmp, moof_sz); c->len += moof_sz;          /* moof */
        memcpy(c->buf + c->len, tmp + moof_sz, total - moof_sz);           /* mdat */
        c->len += total - moof_sz;
        c->name = "invalid-moof-moof";
        c->exp_valid = false; c->exp_reason = MOQ_CMAF_ERR_MALFORMED;
        c->exp_chunk_count = 0; c->exp_track_id = tid;
        break;
    }
    case K_DANGLING_MOOF: {
        /* a single well-formed moof with no following mdat */
        uint8_t tmp[512];
        (void)moq_test_build_cmaf_chunk(tmp, tid, FLAGS_SYNC, true, false, mb);
        size_t moof_sz = rd32be(tmp);
        memcpy(c->buf + c->len, tmp, moof_sz); c->len += moof_sz;
        c->name = "invalid-dangling-moof";
        c->exp_valid = false; c->exp_reason = MOQ_CMAF_ERR_MALFORMED;
        c->exp_chunk_count = 0; c->exp_track_id = tid;
        break;
    }
    case K_STRAY_BOX: {
        /* a free box where a moof is expected, then a valid chunk */
        c->name = "invalid-stray-box";
        c->len += moq_test_box_hdr(c->buf + c->len, 8, "free");
        append_chunk(c, tid, FLAGS_SYNC, true, false, mb);
        c->exp_valid = false; c->exp_reason = MOQ_CMAF_ERR_MALFORMED;
        c->exp_chunk_count = 0; c->exp_track_id = 0;
        break;
    }
    case K_MISSING_MFHD: {
        c->name = "invalid-missing-mfhd";
        append_chunk(c, tid, FLAGS_SYNC, false /*no mfhd*/, false, mb);
        c->exp_valid = false; c->exp_reason = MOQ_CMAF_ERR_MISSING_MFHD;
        c->exp_chunk_count = 0; c->exp_track_id = 0;
        break;
    }
    case K_NO_SAMPLES: {
        /* well-formed moof but trun declares zero samples; the moof's tfhd is
         * read (track_id set) before the mdat phase reports NO_SAMPLES. */
        c->name = "invalid-no-samples";
        c->len += moq_test_build_cmaf_chunk_no_samples(c->buf + c->len, tid, mb);
        c->exp_valid = false; c->exp_reason = MOQ_CMAF_ERR_NO_SAMPLES;
        c->exp_chunk_count = 0; c->exp_track_id = tid;
        break;
    }
    case K_MULTI_TRAF: {
        c->name = "invalid-multi-traf";
        append_chunk(c, tid, FLAGS_SYNC, true, true /*two traf*/, mb);
        c->exp_valid = false; c->exp_reason = MOQ_CMAF_ERR_MULTI_TRACK;
        c->exp_chunk_count = 0; c->exp_track_id = 0;
        break;
    }
    case K_MULTI_TRACK_CHUNKS: {
        /* two valid chunks with different track_ids: the second chunk's tfhd
         * track_ID differs -> MULTI_TRACK after the first chunk completed. */
        uint32_t tid2 = tid + 1;
        c->name = "invalid-multi-track-chunks";
        append_chunk(c, tid, FLAGS_SYNC, true, false, mb);
        append_chunk(c, tid2, FLAGS_SYNC, true, false,
                     (uint8_t)moq_fuzz_rng_below(rng, 256));
        c->exp_valid = false; c->exp_reason = MOQ_CMAF_ERR_MULTI_TRACK;
        c->exp_chunk_count = 1; c->exp_track_id = tid;
        break;
    }
    case K_TRACK_ID_MISMATCH: {
        c->name = "invalid-track-id-mismatch";
        append_chunk(c, tid, FLAGS_SYNC, true, false, mb);
        c->use_init = true; c->init_track_id = tid + 1;   /* != chunk track_id */
        c->exp_valid = false; c->exp_reason = MOQ_CMAF_ERR_TRACK_ID_MISMATCH;
        c->exp_chunk_count = 1; c->exp_track_id = tid;
        break;
    }
    case K_NO_CHUNK: {
        /* an empty object (non-NULL data, zero length) */
        c->name = "invalid-no-chunk";
        c->len = 0;
        c->exp_valid = false; c->exp_reason = MOQ_CMAF_ERR_NO_CHUNK;
        c->exp_chunk_count = 0; c->exp_track_id = 0;
        break;
    }
    case K_TRUNCATED_BOX: {
        /* a moof box whose declared size exceeds the object -> read error */
        c->name = "invalid-truncated-box";
        c->len += moq_test_box_hdr(c->buf + c->len, 64 /*> 8 bytes present*/, "moof");
        c->exp_valid = false; c->exp_reason = MOQ_CMAF_ERR_MALFORMED;
        c->exp_chunk_count = 0; c->exp_track_id = 0;
        break;
    }
    case K_COUNT:
    default:
        c->name = "noop";
        c->exp_valid = false; c->exp_reason = MOQ_CMAF_ERR_NO_CHUNK;
        break;
    }
}

static void wr32be(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

/* Soft oracle for a mutated CMAF object: validate must never crash; the result
 * is either OK+valid or PROTO+invalid; the reason is within the enum range; and
 * if it is accepted, parse_fragment must also accept with coherent first-chunk
 * fields. A flip can produce a different-but-valid object -- that is acceptable. */
static int cmaf_soft(const uint8_t *buf, size_t len)
{
    int failures = 0;
    moq_cmaf_object_report_t rep;
    moq_result_t rc = moq_cmaf_validate_object(NULL, (moq_bytes_t){ buf, len },
                                               &rep);
    MOQ_TEST_CHECK((int)rc == (int)(rep.valid ? MOQ_OK : MOQ_ERR_PROTO));
    MOQ_TEST_CHECK(rep.valid == (rep.reason == MOQ_CMAF_OK));
    MOQ_TEST_CHECK((int)rep.reason >= (int)MOQ_CMAF_OK &&
                   (int)rep.reason <= (int)MOQ_CMAF_ERR_NO_SAMPLES);
    if (rep.valid) {
        moq_cmaf_sample_t sbuf[512];
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, sbuf, 512);
        moq_result_t pr = moq_cmaf_parse_fragment((moq_bytes_t){ buf, len }, &frag);
        MOQ_TEST_CHECK_EQ_INT((int)pr, (int)MOQ_OK);
        if (pr == MOQ_OK) {
            MOQ_TEST_CHECK_EQ_U64(frag.chunk_count, rep.chunk_count);
            MOQ_TEST_CHECK_EQ_U64(frag.track_id, rep.track_id);
            MOQ_TEST_CHECK(frag.sample_count > 0);
        }
    }
    return failures;
}

/* Assert a deterministically-broken object is rejected as MALFORMED. */
static int cmaf_strict_malformed(const uint8_t *buf, size_t len)
{
    int failures = 0;
    moq_cmaf_object_report_t rep;
    moq_result_t rc = moq_cmaf_validate_object(NULL, (moq_bytes_t){ buf, len },
                                               &rep);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_PROTO);
    MOQ_TEST_CHECK(!rep.valid);
    MOQ_TEST_CHECK_EQ_INT((int)rep.reason, (int)MOQ_CMAF_ERR_MALFORMED);
    return failures;
}

/* Build a valid single chunk, then apply one structured mutation and check the
 * appropriate oracle. */
static int cmaf_mutate(moq_fuzz_rng_t *rng, uint64_t seed, int step)
{
    int failures = 0;
    uint32_t tid = 1 + (uint32_t)moq_fuzz_rng_below(rng, 64);
    uint8_t base[512];
    size_t blen = moq_test_build_cmaf_chunk(base, tid, FLAGS_SYNC, true, false,
                                            (uint8_t)moq_fuzz_rng_below(rng, 256));
    uint32_t moof_sz = rd32be(base);            /* first box = moof */
    int kind = (int)moq_fuzz_rng_below(rng, 7);
    const char *kname = "?";
    uint8_t out[1024];

    if (kind == 0) {                            /* truncate at a box boundary */
        kname = "truncate-boundary";
        size_t len = moq_fuzz_rng_bool(rng) ? moof_sz : 0;
        memcpy(out, base, blen);
        failures += cmaf_soft(out, len);
    } else if (kind == 1) {                     /* truncate inside a payload */
        kname = "truncate-payload";
        size_t len = (size_t)moq_fuzz_rng_below(rng, blen + 1);
        memcpy(out, base, blen);
        failures += cmaf_soft(out, len);
    } else if (kind == 2) {                     /* flip a byte of the box type */
        kname = "flip-type";
        memcpy(out, base, blen);
        out[4 + moq_fuzz_rng_below(rng, 4)] ^= (uint8_t)(1u << moq_fuzz_rng_below(rng, 8));
        failures += cmaf_soft(out, blen);
    } else if (kind == 3) {                     /* corrupt the box size */
        kname = "corrupt-size";
        memcpy(out, base, blen);
        moq_fuzz_flip_byte(out, 4, rng);        /* flip within the size field */
        failures += cmaf_soft(out, blen);
    } else if (kind == 4) {                     /* reorder: mdat before moof */
        kname = "mdat-first";
        size_t mlen = blen - moof_sz;
        memcpy(out, base + moof_sz, mlen);      /* mdat */
        memcpy(out + mlen, base, moof_sz);      /* moof */
        failures += cmaf_strict_malformed(out, blen);
    } else if (kind == 5) {                     /* oversized top-level box */
        kname = "oversize-box";
        memcpy(out, base, blen);
        wr32be(out, (uint32_t)blen + 64);       /* moof claims more than present */
        failures += cmaf_strict_malformed(out, blen);
    } else {                                    /* drop the trailing mdat */
        kname = "drop-tail";
        memcpy(out, base, moof_sz);             /* dangling moof */
        failures += cmaf_strict_malformed(out, moof_sz);
    }

    if (failures)
        fprintf(stderr, "  (seed=0x%llx step=%d: cmaf mutate '%s')\n",
                (unsigned long long)seed, step, kname);
    return failures;
}

static int cmaf_one_case(moq_fuzz_rng_t *rng, uint64_t seed, int step,
                         uint32_t mutate_permille)
{
    int failures = 0;
    cmaf_case_t c;
    gen_case(rng, &c);

    moq_cmaf_init_info_t init;
    moq_cmaf_init_info_init(&init);
    init.track_id = c.init_track_id;

    moq_bytes_t obj = { c.buf, c.len };
    moq_cmaf_object_report_t rep;
    moq_result_t rc = moq_cmaf_validate_object(c.use_init ? &init : NULL,
                                               obj, &rep);

    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)(c.exp_valid ? MOQ_OK : MOQ_ERR_PROTO));
    MOQ_TEST_CHECK_EQ_INT((int)rep.valid, (int)c.exp_valid);
    MOQ_TEST_CHECK_EQ_INT((int)rep.reason, (int)c.exp_reason);
    MOQ_TEST_CHECK_EQ_U64(rep.chunk_count, c.exp_chunk_count);
    MOQ_TEST_CHECK_EQ_U64(rep.track_id, c.exp_track_id);
    if (c.check_sap)
        MOQ_TEST_CHECK_EQ_INT((int)rep.sap_type, (int)c.exp_sap);

    /* Valid objects: parse_fragment normalizes the first chunk and the mdat
     * borrows from the input (not copied). */
    if (c.exp_valid) {
        moq_cmaf_sample_t sbuf[512];
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, sbuf, 512);
        moq_result_t pr = moq_cmaf_parse_fragment(obj, &frag);
        MOQ_TEST_CHECK_EQ_INT((int)pr, (int)MOQ_OK);
        if (pr == MOQ_OK) {
            MOQ_TEST_CHECK_EQ_U64(frag.chunk_count, c.exp_chunk_count);
            MOQ_TEST_CHECK_EQ_U64(frag.track_id, c.exp_track_id);
            MOQ_TEST_CHECK(frag.sample_count > 0);
            /* first chunk's 1-byte mdat, borrowed from the object buffer */
            MOQ_TEST_CHECK(frag.mdat.len == 1 &&
                frag.mdat.data >= c.buf &&
                frag.mdat.data + frag.mdat.len <= c.buf + c.len &&
                frag.mdat.data[0] == c.first_mdat_byte);
        }
    }

    if (failures)
        fprintf(stderr, "  (seed=0x%llx step=%d: cmaf case '%s')\n",
                (unsigned long long)seed, step, c.name ? c.name : "?");

    /* Optional structured mutation (operates on a freshly built valid object). */
    if (moq_fuzz_hit_permille(rng, mutate_permille))
        failures += cmaf_mutate(rng, seed, step);

    return failures;
}

int moq_fuzz_cmaf_run_seed(uint64_t seed, int steps, uint32_t mutate_permille,
                           const char *argv0)
{
    int failures = 0;
    moq_fuzz_rng_t rng = { seed };
    for (int step = 0; step < steps; step++)
        failures += cmaf_one_case(&rng, seed, step, mutate_permille);
    if (failures) moq_fuzz_print_replay(argv0, seed, steps, mutate_permille);
    return failures;
}
