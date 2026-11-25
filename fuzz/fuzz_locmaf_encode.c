/*
 * libFuzzer target for the LOCMAF encoder (media/locmaf/include/moq/locmaf.h).
 *
 * Byte 0 picks the mode and the track (clear, cenc 8-byte IV, cbcs).
 *   Raw mode: the rest of the input is one CMAF chunk.
 *   Sequence mode: the rest seeds the test generator, which draws a group
 *   of up to eight muxer-style chunks.
 *
 * Invariants under test:
 *   1. moq_locmaf_encoder_check() and moq_locmaf_encode_chunk() return only
 *      OK, PROTO, UNSUPPORTED or LIMIT for a chunk, agree on acceptance and
 *      reason, and an accepted chunk's encoding fits the check() bound.
 *   2. An encoded chunk decodes, and its canonical chunk equals the chunk
 *      encoded alone as a full header, decoded and written (canonicalize).
 *   3. Re-encoding the canonical chunks, in the same Locations, gives the
 *      same Objects.
 *   4. After every chunk the decoder's reference equals the encoder's.
 */

#include "locmaf_fixtures.h"

#include <stddef.h>
#include <stdint.h>

#define MAXB 8192

static bool canonicalize(const moq_locmaf_track_t *t, const uint8_t *x, size_t n,
                         uint8_t *out, size_t *out_n)
{
    lm_enc_t e;
    lm_dec_t d;
    uint8_t obj[MAXB];
    size_t on = 0;
    moq_locmaf_reason_t why;
    bool ok = false;
    if (lm_enc_open(&e, t, 64, 256, 4) != MOQ_OK) __builtin_trap();
    if (lm_dec_open(&d, t, 64, 256, 4) != MOQ_OK) __builtin_trap();
    moq_locmaf_chunk_t ch;
    if (moq_locmaf_encode_chunk(&e.e, 0, 0, (moq_bytes_t){ x, n }, obj, MAXB, &on, &why) == MOQ_OK) {
        if (moq_locmaf_decoder_push(&d.d, 0, 0, (moq_bytes_t){ obj, on }, &ch, sizeof(ch), &why) != MOQ_OK)
            __builtin_trap();
        if (moq_locmaf_write_cmaf(t, &ch, out, MAXB, out_n) != MOQ_OK) __builtin_trap();
        ok = true;
    }
    lm_enc_close(&e);
    lm_dec_close(&d);
    return ok;
}

static bool refs_equal(const moq_locmaf_ref_state_t *a, const moq_locmaf_ref_state_t *b)
{
    if (a->_present != b->_present || memcmp(a->_scalar, b->_scalar, sizeof(a->_scalar)))
        return false;
    for (unsigned id = 1; id <= 15; id += 2) {
        if (!(a->_present & (1u << id))) continue;
        if (a->_count[id] != b->_count[id]) return false;
        for (uint32_t i = 0; i < a->_count[id]; i++) {
            int64_t x, y;
            switch (id) {
            case 1:  x = a->_sizes[i]; y = b->_sizes[i]; break;
            case 3:  x = a->_durations[i]; y = b->_durations[i]; break;
            case 5:  x = a->_ctos[i]; y = b->_ctos[i]; break;
            case 7:  x = a->_flags[i]; y = b->_flags[i]; break;
            case 9:  x = a->_ivs[i]; y = b->_ivs[i]; break;
            case 11: x = a->_subsample_counts[i]; y = b->_subsample_counts[i]; break;
            case 13: x = a->_clear[i]; y = b->_clear[i]; break;
            default: x = a->_protected[i]; y = b->_protected[i]; break;
            }
            if (x != y) return false;
        }
    }
    return true;
}

typedef struct item { uint8_t src[MAXB]; size_t n; uint8_t obj[MAXB]; size_t on; } item_t;

static void run(const moq_locmaf_track_t *t, item_t *items, size_t count)
{
    lm_enc_t e;
    lm_dec_t d;
    if (lm_enc_open(&e, t, 64, 256, 4) != MOQ_OK) __builtin_trap();
    if (lm_dec_open(&d, t, 64, 256, 4) != MOQ_OK) __builtin_trap();
    size_t accepted = 0;
    for (size_t k = 0; k < count; k++) {
        item_t *it = &items[k];
        size_t bound = 0;
        moq_locmaf_reason_t r1, r2;
        moq_result_t c = moq_locmaf_encoder_check(&e.e, (moq_bytes_t){ it->src, it->n }, &bound, &r1);
        moq_result_t rc = moq_locmaf_encode_chunk(&e.e, 0, k, (moq_bytes_t){ it->src, it->n },
                                                  it->obj, MAXB, &it->on, &r2);
        if (rc != MOQ_OK && rc != MOQ_ERR_PROTO && rc != MOQ_ERR_UNSUPPORTED &&
            rc != MOQ_ERR_LIMIT)
            __builtin_trap();
        if (c != rc || (rc != MOQ_OK && r1 != r2)) __builtin_trap();
        if (rc != MOQ_OK) { it->on = 0; break; }
        if (it->on > bound) __builtin_trap();
        moq_locmaf_encoder_commit(&e.e);
        moq_locmaf_chunk_t ch;
        if (moq_locmaf_decoder_push(&d.d, 0, k, (moq_bytes_t){ it->obj, it->on }, &ch,
                                    sizeof(ch), &r2) != MOQ_OK)
            __builtin_trap();
        uint8_t w[MAXB], canon[MAXB];
        size_t wn = 0, cn = 0;
        if (moq_locmaf_write_cmaf(t, &ch, w, MAXB, &wn) != MOQ_OK) __builtin_trap();
        if (!canonicalize(t, it->src, it->n, canon, &cn)) __builtin_trap();
        if (wn != cn || memcmp(w, canon, wn) != 0) __builtin_trap();
        if (!refs_equal(&d.d._ref, &e.e._ref[e.e._committed_slot])) __builtin_trap();
        memcpy(it->src, canon, cn);                 /* for invariant 3 */
        it->n = cn;
        accepted++;
    }
    lm_enc_close(&e);
    lm_dec_close(&d);

    if (lm_enc_open(&e, t, 64, 256, 4) != MOQ_OK) __builtin_trap();
    for (size_t k = 0; k < accepted; k++) {
        uint8_t obj[MAXB];
        size_t on = 0;
        moq_locmaf_reason_t why;
        if (moq_locmaf_encode_chunk(&e.e, 0, k, (moq_bytes_t){ items[k].src, items[k].n }, obj,
                                    MAXB, &on, &why) != MOQ_OK)
            __builtin_trap();
        moq_locmaf_encoder_commit(&e.e);
        if (on != items[k].on || memcmp(obj, items[k].obj, on) != 0) __builtin_trap();
    }
    lm_enc_close(&e);
}

static item_t items[8];

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < 1 || size > MAXB) return 0;
    moq_locmaf_track_t t = (data[0] % 3) == 1 ? lm_track_p8()
                         : (data[0] % 3) == 2 ? lm_track_p0() : lm_track_a();
    if (data[0] & 0x80) {
        memcpy(items[0].src, data + 1, size - 1);
        items[0].n = size - 1;
        run(&t, items, 1);
        return 0;
    }
    uint64_t seed = 1469598103934665603ull;
    for (size_t i = 1; i < size; i++) seed = (seed ^ data[i]) * 1099511628211ull;
    lm_rng_t rng = { seed };
    size_t count = 1 + (size_t)(data[0] >> 4) % 8;
    uint64_t next = 0;
    for (size_t k = 0; k < count; k++) {
        lm_gen_chunk_t g;
        items[k].n = lm_gen_source(&rng, &t, next, k == 0, &g, items[k].src, MAXB);
        next = g.bmdt;
        for (uint32_t i = 0; i < g.n; i++) next += g.durs[i];
    }
    run(&t, items, count);
    return 0;
}
