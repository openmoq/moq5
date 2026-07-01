/*
 * Seeded LOC property test (generate-only). See media_fuzz.h.
 *
 * Generates valid LOC-01 header models and checks the producer/consumer round
 * trip (encode -> parse -> field-equal) plus deterministic re-encode, against
 * the shared per-case counting allocator. A generated "valid" the encoder
 * rejects is a generator bug, reported with the seed.
 */

#include "media_fuzz.h"

#include <moq/types.h>
#include <moq/rcbuf.h>
#include <moq/loc.h>
#include <moq/kvp.h>

#include "../unit/test_support.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LOC_MAX_VCFG 32

static bool loc_headers_equal(const moq_loc_headers_t *a,
                              const moq_loc_headers_t *b)
{
    if (a->has_timestamp != b->has_timestamp) return false;
    if (a->has_timestamp && a->timestamp != b->timestamp) return false;

    if (a->has_video_frame_marking != b->has_video_frame_marking) return false;
    if (a->has_video_frame_marking) {
        const moq_loc_video_frame_marking_t *x = &a->video_frame_marking;
        const moq_loc_video_frame_marking_t *y = &b->video_frame_marking;
        if (x->start_of_frame  != y->start_of_frame  ||
            x->end_of_frame    != y->end_of_frame    ||
            x->independent     != y->independent     ||
            x->discardable     != y->discardable     ||
            x->base_layer_sync != y->base_layer_sync ||
            x->temporal_id     != y->temporal_id     ||
            x->has_layer_id    != y->has_layer_id    ||
            (x->has_layer_id && x->layer_id != y->layer_id))
            return false;
    }

    if (a->has_audio_level != b->has_audio_level) return false;
    if (a->has_audio_level) {
        if (a->audio_level.voice_activity != b->audio_level.voice_activity ||
            a->audio_level.level          != b->audio_level.level)
            return false;
    }

    if (a->has_video_config != b->has_video_config) return false;
    if (a->has_video_config) {
        if (a->video_config.len != b->video_config.len) return false;
        if (a->video_config.len &&
            memcmp(a->video_config.data, b->video_config.data,
                   a->video_config.len) != 0)
            return false;
    }
    return true;
}

/* Generate a valid LOC-01 header model. video_config bytes are backed by the
 * caller-provided vcfg[LOC_MAX_VCFG] buffer (must outlive the encode + parse +
 * compare). A track is modeled as video OR audio (realistic; both-set is not a
 * real LOC shape). Timescale/LOC-02 fields are not generated (LOC-01 only). */
static void loc_gen(moq_fuzz_rng_t *rng, moq_loc_headers_t *h, uint8_t *vcfg)
{
    moq_loc_headers_init(h);

    if (moq_fuzz_rng_bool(rng)) {
        h->has_timestamp = true;
        /* keep < 2^62 so the QUIC-varint encode is valid */
        h->timestamp = moq_fuzz_rng_next(rng) & ((1ULL << 62) - 1);
    }

    if (moq_fuzz_rng_bool(rng)) {
        h->has_video_frame_marking = true;
        moq_loc_video_frame_marking_t *m = &h->video_frame_marking;
        m->start_of_frame  = moq_fuzz_rng_bool(rng);
        m->end_of_frame    = moq_fuzz_rng_bool(rng);
        m->independent     = moq_fuzz_rng_bool(rng);
        m->discardable     = moq_fuzz_rng_bool(rng);
        m->base_layer_sync = moq_fuzz_rng_bool(rng);
        m->temporal_id     = (uint8_t)moq_fuzz_rng_below(rng, 8);
        /* The LOC-01 frame marking is a varint whose optional layer_id is the
         * low byte; its presence is signaled ONLY by a non-zero high byte (the
         * flags + temporal_id). So a layer_id cannot round-trip when every flag
         * is clear and temporal_id==0 -- the value is < 256 and decodes as "no
         * layer id". Offer has_layer_id only when the high byte is non-zero, so
         * generated models stay round-trippable. (This is an inherent LOC-01
         * wire ambiguity, surfaced by this harness and reported separately --
         * not a generator concession to a defect we can otherwise represent.) */
        bool first_nonzero = m->start_of_frame || m->end_of_frame ||
            m->independent || m->discardable || m->base_layer_sync ||
            (m->temporal_id != 0);
        if (first_nonzero && moq_fuzz_rng_bool(rng)) {
            m->has_layer_id = true;
            m->layer_id = (uint8_t)moq_fuzz_rng_below(rng, 256);
        }
        if (moq_fuzz_rng_bool(rng)) {
            size_t n = (size_t)moq_fuzz_rng_below(rng, LOC_MAX_VCFG) + 1;
            for (size_t i = 0; i < n; i++)
                vcfg[i] = (uint8_t)moq_fuzz_rng_below(rng, 256);
            h->has_video_config = true;
            h->video_config = (moq_bytes_t){ vcfg, n };
        }
    } else {
        h->has_audio_level = true;
        h->audio_level.voice_activity = moq_fuzz_rng_bool(rng);
        h->audio_level.level = (uint8_t)moq_fuzz_rng_below(rng, 128);
    }
}

/* Soft oracle for a mutated LOC blob: the parser must never crash; a reject is
 * acceptable (a random mutation may simply be invalid); but if it ACCEPTS, the
 * parsed model must survive re-encode->reparse unchanged (an idempotent fixpoint
 * -- proves the accepted bytes round-trip coherently). */
static int loc_soft(const moq_alloc_t *alloc, const uint8_t *buf, size_t len)
{
    int failures = 0;
    moq_loc_headers_t p1;
    if (moq_loc_parse(MOQ_LOC_PROFILE_01, (moq_bytes_t){ buf, len }, &p1)
        != MOQ_OK)
        return 0;   /* reject is fine */
    /* A parsed model must re-encode (an empty model legitimately encodes to no
     * bytes: MOQ_OK with a NULL rcbuf -- treated as a zero-length property set). */
    moq_rcbuf_t *re = NULL;
    moq_result_t er = moq_loc_encode(alloc, MOQ_LOC_PROFILE_01, &p1, &re);
    MOQ_TEST_CHECK(er == MOQ_OK);
    if (er == MOQ_OK) {
        moq_bytes_t rb = re ? (moq_bytes_t){ moq_rcbuf_data(re), moq_rcbuf_len(re) }
                            : (moq_bytes_t){ NULL, 0 };
        moq_loc_headers_t p2;
        moq_result_t rc2 = moq_loc_parse(MOQ_LOC_PROFILE_01, rb, &p2);
        MOQ_TEST_CHECK(rc2 == MOQ_OK);
        if (rc2 == MOQ_OK)
            MOQ_TEST_CHECK(loc_headers_equal(&p1, &p2));
        if (re) moq_rcbuf_decref(re);
    }
    return failures;
}

/* Apply one structured mutation to a successfully-encoded LOC blob and check the
 * appropriate oracle. Scratch uses malloc (not the counting allocator, which is
 * reserved for the library calls under leak-balance). */
static int loc_mutate(const moq_alloc_t *alloc, moq_fuzz_rng_t *rng,
                      uint64_t seed, int step, const moq_loc_headers_t *model,
                      const uint8_t *blob, size_t blen)
{
    int failures = 0;
    size_t cap = blen + 32;
    uint8_t *buf = (uint8_t *)malloc(cap);
    if (!buf) { MOQ_TEST_CHECK(false); return failures; }  /* never false-green */
    memcpy(buf, blob, blen);
    int kind = (int)moq_fuzz_rng_below(rng, 4);
    const char *kname = "?";

    if (kind == 0) {                         /* truncate at a random position */
        kname = "truncate";
        size_t len = (size_t)moq_fuzz_rng_below(rng, blen + 1);
        failures += loc_soft(alloc, buf, len);
    } else if (kind == 1) {                  /* flip one byte */
        kname = "flip";
        moq_fuzz_flip_byte(buf, blen, rng);
        failures += loc_soft(alloc, buf, blen);
    } else if (kind == 2) {                  /* append unknown well-formed KVP */
        kname = "append-unknown-kvp";
        uint64_t last_type = 0;
        moq_kvp_decoder_t d;
        moq_kvp_decoder_init(&d, buf, blen);
        moq_kvp_entry_t e;
        while (moq_kvp_decode_next(&d, &e) == MOQ_OK) last_type = e.type;
        /* even type > all known LOC types (<= 0x0d) -> parser skips it. */
        size_t n = moq_kvp_encode_varint_entry(last_type, 0x20,
            moq_fuzz_rng_below(rng, 1u << 20), buf + blen, cap - blen);
        MOQ_TEST_CHECK(n > 0);
        if (n > 0) {
            moq_loc_headers_t p1;
            moq_result_t rc = moq_loc_parse(MOQ_LOC_PROFILE_01,
                (moq_bytes_t){ buf, blen + n }, &p1);
            MOQ_TEST_CHECK(rc == MOQ_OK);          /* unknown KVP ignored */
            if (rc == MOQ_OK)
                MOQ_TEST_CHECK(loc_headers_equal(model, &p1));
        }
    } else {                                 /* corrupt KVP value length */
        kname = "corrupt-len";
        /* Append an odd-type (byte-string) entry whose declared length (5)
         * exceeds the bytes present (0) -> the KVP decoder must reject. The
         * type delta and length are each < 64 so they are 1-byte QUIC varints. */
        uint64_t last_type = 0;
        moq_kvp_decoder_t d;
        moq_kvp_decoder_init(&d, buf, blen);
        moq_kvp_entry_t e;
        while (moq_kvp_decode_next(&d, &e) == MOQ_OK) last_type = e.type;
        uint64_t otype = (last_type | 1u) + 2u;   /* odd, ascending, unknown */
        uint64_t delta = otype - last_type;
        if (delta < 64) {
            buf[blen]     = (uint8_t)delta;        /* type delta varint */
            buf[blen + 1] = 0x05;                  /* length 5, no value bytes */
            moq_loc_headers_t p1;
            moq_result_t rc = moq_loc_parse(MOQ_LOC_PROFILE_01,
                (moq_bytes_t){ buf, blen + 2 }, &p1);
            MOQ_TEST_CHECK(rc != MOQ_OK);          /* truncated value -> reject */
        }
    }

    free(buf);
    if (failures)
        fprintf(stderr, "  (seed=0x%llx step=%d: loc mutate '%s')\n",
                (unsigned long long)seed, step, kname);
    return failures;
}

/* One LOC case: generate -> encode (must succeed) -> parse -> field-equal, plus
 * a determinism check (the same model re-encodes to identical bytes), then an
 * optional structured mutation gated on mutate_permille. */
static int loc_one_case(const moq_alloc_t *alloc, moq_fuzz_rng_t *rng,
                        uint64_t seed, int step, uint32_t mutate_permille)
{
    int failures = 0;
    uint8_t vcfg[LOC_MAX_VCFG];
    moq_loc_headers_t model;
    loc_gen(rng, &model, vcfg);

    moq_rcbuf_t *enc = NULL;
    moq_result_t rc = moq_loc_encode(alloc, MOQ_LOC_PROFILE_01, &model, &enc);
    MOQ_TEST_CHECK(rc == MOQ_OK && enc != NULL);   /* generated valid must encode */
    if (rc != MOQ_OK || !enc) {
        if (enc) moq_rcbuf_decref(enc);
        fprintf(stderr, "  (seed=0x%llx step=%d: loc encode rc=%d)\n",
                (unsigned long long)seed, step, (int)rc);
        return failures;
    }

    moq_bytes_t bytes = { moq_rcbuf_data(enc), moq_rcbuf_len(enc) };
    moq_loc_headers_t back;
    moq_result_t prc = moq_loc_parse(MOQ_LOC_PROFILE_01, bytes, &back);
    MOQ_TEST_CHECK(prc == MOQ_OK);
    if (prc == MOQ_OK)
        MOQ_TEST_CHECK(loc_headers_equal(&model, &back));

    /* Encode is deterministic: the same model yields identical bytes. */
    moq_rcbuf_t *enc2 = NULL;
    moq_result_t rc2 = moq_loc_encode(alloc, MOQ_LOC_PROFILE_01, &model, &enc2);
    MOQ_TEST_CHECK(rc2 == MOQ_OK && enc2 != NULL);
    if (rc2 == MOQ_OK && enc2) {
        size_t n1 = moq_rcbuf_len(enc), n2 = moq_rcbuf_len(enc2);
        MOQ_TEST_CHECK(n1 == n2 &&
            (n1 == 0 || memcmp(moq_rcbuf_data(enc), moq_rcbuf_data(enc2), n1) == 0));
    }
    if (enc2) moq_rcbuf_decref(enc2);

    /* Optional structured mutation of the just-encoded blob. */
    if (moq_fuzz_hit_permille(rng, mutate_permille))
        failures += loc_mutate(alloc, rng, seed, step, &model,
                               moq_rcbuf_data(enc), moq_rcbuf_len(enc));

    moq_rcbuf_decref(enc);

    if (failures)
        fprintf(stderr, "  (seed=0x%llx step=%d: loc case)\n",
                (unsigned long long)seed, step);
    return failures;
}

int moq_fuzz_loc_run_seed(uint64_t seed, int steps, uint32_t mutate_permille,
                          const char *argv0)
{
    int failures = 0;
    moq_fuzz_rng_t rng = { seed };
    for (int step = 0; step < steps; step++) {
        moq_fuzz_alloc_state_t as = { 0 };
        moq_alloc_t alloc;
        moq_fuzz_alloc_init(&alloc, &as);
        failures += loc_one_case(&alloc, &rng, seed, step, mutate_permille);
        if (as.balance != 0) {
            fprintf(stderr, "FAIL seed=0x%llx step=%d: loc alloc balance=%lld\n",
                    (unsigned long long)seed, step, (long long)as.balance);
            failures++;
        }
    }
    if (failures) moq_fuzz_print_replay(argv0, seed, steps, mutate_permille);
    return failures;
}
