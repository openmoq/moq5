/*
 * The round-trip invariants over seeded chunk sequences, on every track
 * preset:
 *   1. write(decode(encode(x))) == canonicalize(x), where canonicalize(x)
 *      writes the effective values a lone full header carries (a fresh
 *      encoder and decoder per chunk);
 *   2. canonicalize is idempotent;
 *   3. a canonical input survives the chain unchanged;
 *   4. re-encoding the canonical chunks gives the same Objects;
 *   5. every legal re-representation decodes to the same canonical bytes:
 *      reversed field order, the longest vi64 forms, unknown fields, a full
 *      header at every Object with redundant defaults, explicit zero
 *      deltas, and inherited-list truncation;
 *   6. after every chunk the decoder's reference equals the encoder's;
 *   7. the CMAF parser reads the same sample table from the source and from
 *      the canonical chunk, and it agrees with the decoder's;
 * and the encoder_check() bound holds for every encoding.
 * A failure prints its seed.
 */
#include "locmaf_fixtures.h"

#include <moq/cmaf.h>
#include <moq/vi64.h>

#define MAXB 2048

typedef struct buf { uint8_t b[MAXB]; size_t n; } buf_t;

/* -- A test-only Object re-encoder -------------------------------------------- */

typedef struct tuple {
    uint64_t       id;
    uint64_t       value;                /* even IDs */
    const uint8_t *bytes;                /* odd IDs */
    size_t         len;
} tuple_t;

typedef struct parsed {
    uint64_t       kind;
    size_t         gen_count;
    const uint8_t *gen_start;            /* the genBox elements, verbatim */
    size_t         gen_len;
    tuple_t        t[64];
    size_t         count;
    const uint8_t *mdat;
    size_t         mdat_len;
} parsed_t;

static bool parse_obj(const uint8_t *p, size_t n, parsed_t *o)
{
    memset(o, 0, sizeof(*o));
    size_t pos = 0;
    uint64_t t = 0, v = 0;
    o->gen_start = p;
    for (;;) {
        size_t k = moq_vi64_decode(p + pos, n - pos, &t);
        if (!k) return false;
        if (t != 1) { o->gen_len = pos; pos += k; break; }
        pos += k;
        k = moq_vi64_decode(p + pos, n - pos, &v);
        pos += k + (size_t)v;
        o->gen_count++;
    }
    o->kind = t;
    if (t == 4) return true;
    size_t k = moq_vi64_decode(p + pos, n - pos, &v);
    pos += k;
    size_t end = pos + (size_t)v;
    while (pos < end && o->count < 64) {
        tuple_t *u = &o->t[o->count++];
        pos += moq_vi64_decode(p + pos, end - pos, &u->id);
        pos += moq_vi64_decode(p + pos, end - pos, &v);
        if (u->id & 1) { u->bytes = p + pos; u->len = (size_t)v; pos += (size_t)v; }
        else u->value = v;
    }
    o->mdat = p + end;
    o->mdat_len = n - end;
    return true;
}

static void put_vi(buf_t *b, uint64_t v, bool longest)
{
    if (longest) {
        b->b[b->n++] = 0xFF;
        for (int i = 7; i >= 0; i--) b->b[b->n++] = (uint8_t)(v >> (8 * i));
    } else {
        b->n += moq_vi64_encode(v, b->b + b->n, MAXB - b->n);
    }
}

static void put_tuple(buf_t *b, const tuple_t *u, bool longest)
{
    put_vi(b, u->id, longest);
    if (!(u->id & 1)) { put_vi(b, u->value, longest); return; }
    if (!longest || u->id == 9 || u->id > 27) {
        put_vi(b, u->len, longest);
        memcpy(b->b + b->n, u->bytes, u->len);
        b->n += u->len;
        return;
    }
    /* a list: each element in its longest form */
    size_t count = 0, pos = 0;
    uint64_t v = 0;
    while (pos < u->len) { pos += moq_vi64_decode(u->bytes + pos, u->len - pos, &v); count++; }
    put_vi(b, count * 9, true);
    pos = 0;
    while (pos < u->len) {
        pos += moq_vi64_decode(u->bytes + pos, u->len - pos, &v);
        put_vi(b, v, true);
    }
}

static void build_obj(const parsed_t *o, const tuple_t *t, size_t count, bool longest,
                      buf_t *out)
{
    out->n = 0;
    if (longest) {
        /* genBoxes with longest element type and box_size */
        size_t pos = 0;
        while (pos < o->gen_len) {
            uint64_t ty = 0, size = 0;
            pos += moq_vi64_decode(o->gen_start + pos, o->gen_len - pos, &ty);
            pos += moq_vi64_decode(o->gen_start + pos, o->gen_len - pos, &size);
            put_vi(out, ty, true);
            put_vi(out, size, true);
            memcpy(out->b + out->n, o->gen_start + pos, (size_t)size);
            out->n += (size_t)size;
            pos += (size_t)size;
        }
    } else {
        memcpy(out->b, o->gen_start, o->gen_len);
        out->n = o->gen_len;
    }
    buf_t props = { .n = 0 };
    for (size_t i = 0; i < count; i++) put_tuple(&props, &t[i], longest);
    put_vi(out, o->kind, longest);
    put_vi(out, props.n, longest);
    memcpy(out->b + out->n, props.b, props.n);
    out->n += props.n;
    memcpy(out->b + out->n, o->mdat, o->mdat_len);
    out->n += o->mdat_len;
}

typedef enum {
    V_REVERSED, V_LONGEST, V_UNKNOWN, V_FULL_REDUNDANT, V_ZERO_DELTA, V_TRUNCATION,
    V_COUNT
} variant_t;

static const char *variant_name[V_COUNT] = {
    "reversed", "longest", "unknown", "full+redundant", "zero-delta", "truncation"
};

/* Re-represent one Object; full is the chunk's lone full-header Object (for
 * V_FULL_REDUNDANT), prev_present the fields of the decoder's reference
 * before it (for V_TRUNCATION). Returns false when the variant does not
 * apply. */
static bool variant(variant_t v, const moq_locmaf_track_t *trk, const uint8_t *obj,
                    size_t n, const uint8_t *full, size_t full_n, uint32_t prev_present,
                    buf_t *out)
{
    parsed_t o;
    if (v == V_FULL_REDUNDANT) { obj = full; n = full_n; }
    if (!parse_obj(obj, n, &o) || o.kind == 4) return false;
    tuple_t t[72];
    size_t c = 0;
    switch (v) {
    case V_REVERSED:
        for (size_t i = o.count; i-- > 0;) t[c++] = o.t[i];
        build_obj(&o, t, c, false, out);
        return o.count > 1;
    case V_LONGEST:
        build_obj(&o, o.t, o.count, true, out);
        return true;
    case V_UNKNOWN: {
        static const uint8_t xy[2] = { 'x', 'y' };
        t[c++] = (tuple_t){ 18, 5, NULL, 0 };
        for (size_t i = 0; i < o.count; i++) t[c++] = o.t[i];
        t[c++] = (tuple_t){ 19, 0, xy, 2 };
        build_obj(&o, t, c, false, out);
        return true;
    }
    case V_FULL_REDUNDANT: {
        uint32_t has = 0;
        uint64_t nsamp = 0;
        for (size_t i = 0; i < o.count; i++) {
            if (o.t[i].id < 32) has |= 1u << o.t[i].id;
            if (o.t[i].id == 14) nsamp = o.t[i].value;
            t[c++] = o.t[i];
        }
        /* strict-cmf2 style: the four tfhd defaults even when they equal trex
         * (ID 6 only for n > 1, as section 11.2 requires) */
        if (!(has & (1u << 2))) t[c++] = (tuple_t){ 2, trk->trex_default_sample_description_index, NULL, 0 };
        if (!(has & (1u << 4)) && !(has & (1u << 3)))
            t[c++] = (tuple_t){ 4, trk->trex_default_sample_duration, NULL, 0 };
        if (!(has & (1u << 6)) && !(has & (1u << 1)) && nsamp > 1)
            t[c++] = (tuple_t){ 6, trk->trex_default_sample_size, NULL, 0 };
        if (!(has & (1u << 8)) && !(has & (1u << 7)))
            t[c++] = (tuple_t){ 8, trk->trex_default_sample_flags, NULL, 0 };
        build_obj(&o, t, c, false, out);
        return true;
    }
    case V_ZERO_DELTA: {
        if (o.kind != 3) return false;
        bool has14 = false;
        for (size_t i = 0; i < o.count; i++) { has14 |= o.t[i].id == 14; t[c++] = o.t[i]; }
        if (has14) return false;
        t[c++] = (tuple_t){ 14, 0, NULL, 0 };
        build_obj(&o, t, c, false, out);
        return true;
    }
    case V_TRUNCATION: {
        if (o.kind != 3) return false;
        bool shrink = false, dropped = false;
        for (size_t i = 0; i < o.count; i++)
            if (o.t[i].id == 14 && (o.t[i].value & 1)) shrink = true;
        for (size_t i = 0; i < o.count; i++) {
            const tuple_t *u = &o.t[i];
            /* all-zero element deltas against a longer list in the
             * reference: the receiver's truncation gives the same list */
            bool zero_list = shrink && (u->id == 1 || u->id == 3 || u->id == 5 ||
                                        u->id == 7 || u->id == 11) && u->len > 0 &&
                             (prev_present & (1u << u->id));
            for (size_t k = 0; zero_list && k < u->len; k++) zero_list = u->bytes[k] == 0;
            if (zero_list) { dropped = true; continue; }
            t[c++] = *u;
        }
        if (!dropped) return false;
        build_obj(&o, t, c, false, out);
        return true;
    }
    default:
        return false;
    }
}

/* -- Helpers -------------------------------------------------------------------- */

static uint64_t seed_now;

static moq_result_t write_chunk(const moq_locmaf_track_t *t, const moq_locmaf_chunk_t *c,
                                buf_t *out)
{
    out->n = 0;
    return moq_locmaf_write_cmaf(t, c, out->b, MAXB, &out->n);
}

/* canonicalize(x): the chunk alone, as a full header, decoded and written. */
static bool canonicalize(const moq_locmaf_track_t *t, const uint8_t *x, size_t xn,
                         buf_t *obj, buf_t *out)
{
    lm_enc_t e;
    lm_dec_t d;
    moq_locmaf_reason_t why;
    bool ok = false;
    if (lm_enc_open(&e, t, 0, 0, 0) != MOQ_OK) return false;
    if (lm_dec_open(&d, t, 0, 0, 0) != MOQ_OK) { lm_enc_close(&e); return false; }
    obj->n = 0;
    if (moq_locmaf_encode_chunk(&e.e, 0, 0, (moq_bytes_t){ x, xn }, obj->b, MAXB, &obj->n,
                                &why) == MOQ_OK) {
        moq_locmaf_chunk_t ch;
        if (moq_locmaf_decoder_push(&d.d, 0, 0, (moq_bytes_t){ obj->b, obj->n }, &ch,
                                    sizeof(ch), &why) == MOQ_OK)
            ok = write_chunk(t, &ch, out) == MOQ_OK;
    }
    lm_enc_close(&e);
    lm_dec_close(&d);
    return ok;
}

/* Invariant 6: the two references, field by field. */
static bool refs_equal(const moq_locmaf_ref_state_t *a, const moq_locmaf_ref_state_t *b)
{
    if (a->_present != b->_present) return false;
    if (memcmp(a->_scalar, b->_scalar, sizeof(a->_scalar)) != 0) return false;
    for (unsigned id = 1; id <= 15; id += 2) {
        if (!(a->_present & (1u << id))) continue;
        if (a->_count[id] != b->_count[id]) return false;
        for (uint32_t i = 0; i < a->_count[id]; i++) {
            bool eq;
            switch (id) {
            case 1:  eq = a->_sizes[i] == b->_sizes[i]; break;
            case 3:  eq = a->_durations[i] == b->_durations[i]; break;
            case 5:  eq = a->_ctos[i] == b->_ctos[i]; break;
            case 7:  eq = a->_flags[i] == b->_flags[i]; break;
            case 9:  eq = a->_ivs[i] == b->_ivs[i]; break;
            case 11: eq = a->_subsample_counts[i] == b->_subsample_counts[i]; break;
            case 13: eq = a->_clear[i] == b->_clear[i]; break;
            default: eq = a->_protected[i] == b->_protected[i]; break;
            }
            if (!eq) return false;
        }
    }
    return true;
}

static uint32_t norm(uint32_t v, uint32_t trex) { return v == 0 ? trex : v; }

/* Invariant 7: parse_fragment of the source and of the canonical chunk, and
 * the decoder's table. parse_fragment ignores trex, so 0 reads as trex. */
static bool tables_agree(const moq_locmaf_track_t *t, const uint8_t *src, size_t sn,
                         const buf_t *canon, const moq_locmaf_chunk_t *ch)
{
    if (ch->sample_count == 0) return true;
    moq_cmaf_sample_t a[8], b[8];
    moq_cmaf_fragment_info_t fa, fb;
    moq_cmaf_fragment_info_init(&fa, a, 8);
    moq_cmaf_fragment_info_init(&fb, b, 8);
    if (moq_cmaf_parse_fragment((moq_bytes_t){ src, sn }, &fa) != MOQ_OK ||
        moq_cmaf_parse_fragment((moq_bytes_t){ canon->b, canon->n }, &fb) != MOQ_OK)
        return false;
    if (fa.sample_count != ch->sample_count || fb.sample_count != ch->sample_count ||
        fa.base_decode_time != ch->base_media_decode_time ||
        fb.base_decode_time != ch->base_media_decode_time)
        return false;
    for (size_t i = 0; i < fa.sample_count; i++) {
        const moq_locmaf_sample_t *s = &ch->samples[i];
        if (norm(a[i].duration, t->trex_default_sample_duration) != s->duration ||
            norm(b[i].duration, t->trex_default_sample_duration) != s->duration ||
            norm(a[i].size, t->trex_default_sample_size) != s->size ||
            norm(b[i].size, t->trex_default_sample_size) != s->size ||
            norm(a[i].flags, t->trex_default_sample_flags) != s->flags ||
            norm(b[i].flags, t->trex_default_sample_flags) != s->flags ||
            a[i].composition_offset != s->composition_offset ||
            b[i].composition_offset != s->composition_offset)
            return false;
    }
    return true;
}

#define FAIL_AT(what) do { \
    fprintf(stderr, "FAIL: seed %llu group %llu object %llu: %s\n", \
            (unsigned long long)seed_now, (unsigned long long)g, \
            (unsigned long long)o, (what)); \
    lm_failures++; \
} while (0)

/* -- One group -------------------------------------------------------------------- */

typedef struct stream {
    uint8_t src[8][MAXB];
    size_t  src_n[8];
    buf_t   obj[8];
    buf_t   full[8];
    buf_t   canon[8];
    uint64_t oid[8];
    bool    raw[8];
    size_t  count;
} stream_t;

static void run_group(lm_rng_t *rng, const moq_locmaf_track_t *t, uint64_t g)
{
    static stream_t s;
    memset(&s, 0, sizeof(s));
    lm_enc_t enc;
    lm_dec_t dec;
    CHECK_RC(lm_enc_open(&enc, t, 0, 0, 0), MOQ_OK);
    CHECK_RC(lm_dec_open(&dec, t, 0, 0, 0), MOQ_OK);
    uint64_t next_bmdt = 0, o = 0;
    s.count = 1 + lm_rng_below(rng, 6);
    for (size_t k = 0; k < s.count; k++) {
        if (k && lm_rng_below(rng, 10) == 0) o++;              /* a gap */
        lm_gen_chunk_t gc;
        s.src_n[k] = lm_gen_source(rng, t, next_bmdt, k == 0, &gc, s.src[k], MAXB);
        s.oid[k] = o;
        moq_locmaf_reason_t why;
        size_t bound = 0;
        moq_result_t rc = moq_locmaf_encoder_check(&enc.e, (moq_bytes_t){ s.src[k], s.src_n[k] },
                                                   &bound, &why);
        if (rc != MOQ_OK) { FAIL_AT(moq_locmaf_reason_name(why)); break; }
        if (!canonicalize(t, s.src[k], s.src_n[k], &s.full[k], &s.canon[k])) {
            FAIL_AT("canonicalize");
            break;
        }
        s.raw[k] = k && lm_rng_below(rng, 12) == 0;           /* a rawBoxes Object */
        buf_t *ob = &s.obj[k];
        if (s.raw[k]) {
            rc = moq_locmaf_encode_raw_boxes(&enc.e, g, o,
                (moq_bytes_t){ s.canon[k].b, s.canon[k].n }, ob->b, MAXB, &ob->n, &why);
        } else {
            rc = moq_locmaf_encode_chunk(&enc.e, g, o, (moq_bytes_t){ s.src[k], s.src_n[k] },
                                         ob->b, MAXB, &ob->n, &why);
        }
        if (rc != MOQ_OK) { FAIL_AT(moq_locmaf_reason_name(why)); break; }
        if (!s.raw[k] && (ob->n > bound || s.full[k].n > bound)) FAIL_AT("bound");
        moq_locmaf_encoder_commit(&enc.e);

        moq_locmaf_chunk_t ch;
        rc = moq_locmaf_decoder_push(&dec.d, g, o, (moq_bytes_t){ ob->b, ob->n }, &ch,
                                     sizeof(ch), &why);
        if (rc != MOQ_OK) { FAIL_AT(moq_locmaf_reason_name(why)); break; }
        buf_t w;
        if (write_chunk(t, &ch, &w) != MOQ_OK) { FAIL_AT("write"); break; }
        /* 1 */
        if (w.n != s.canon[k].n || memcmp(w.b, s.canon[k].b, w.n) != 0) FAIL_AT("invariant 1");
        /* 2 */
        buf_t o2, c2;
        if (!canonicalize(t, s.canon[k].b, s.canon[k].n, &o2, &c2) ||
            c2.n != s.canon[k].n || memcmp(c2.b, s.canon[k].b, c2.n) != 0)
            FAIL_AT("invariant 2");
        /* 6 */
        if (!s.raw[k] && !refs_equal(&dec.d._ref, &enc.e._ref[enc.e._committed_slot]))
            FAIL_AT("invariant 6");
        /* 7 */
        if (!s.raw[k] && !tables_agree(t, s.src[k], s.src_n[k], &s.canon[k], &ch))
            FAIL_AT("invariant 7");
        next_bmdt = gc.bmdt;
        for (uint32_t i = 0; i < gc.n; i++) next_bmdt += gc.durs[i];
        o++;
    }
    lm_enc_close(&enc);
    lm_dec_close(&dec);

    /* 3 and 4: the canonical chunks through a second chain */
    CHECK_RC(lm_enc_open(&enc, t, 0, 0, 0), MOQ_OK);
    CHECK_RC(lm_dec_open(&dec, t, 0, 0, 0), MOQ_OK);
    for (size_t k = 0; k < s.count; k++) {
        o = s.oid[k];
        moq_locmaf_reason_t why;
        buf_t ob, w;
        moq_result_t rc = s.raw[k]
            ? moq_locmaf_encode_raw_boxes(&enc.e, g, o, (moq_bytes_t){ s.canon[k].b, s.canon[k].n },
                                          ob.b, MAXB, &ob.n, &why)
            : moq_locmaf_encode_chunk(&enc.e, g, o, (moq_bytes_t){ s.canon[k].b, s.canon[k].n },
                                      ob.b, MAXB, &ob.n, &why);
        if (rc != MOQ_OK) { FAIL_AT("re-encode"); break; }
        moq_locmaf_encoder_commit(&enc.e);
        if (ob.n != s.obj[k].n || memcmp(ob.b, s.obj[k].b, ob.n) != 0) FAIL_AT("invariant 4");
        moq_locmaf_chunk_t ch;
        if (moq_locmaf_decoder_push(&dec.d, g, o, (moq_bytes_t){ ob.b, ob.n }, &ch, sizeof(ch),
                                    &why) != MOQ_OK || write_chunk(t, &ch, &w) != MOQ_OK ||
            w.n != s.canon[k].n || memcmp(w.b, s.canon[k].b, w.n) != 0)
            FAIL_AT("invariant 3");
    }
    lm_enc_close(&enc);
    lm_dec_close(&dec);

    /* 5: each re-representation decodes to the same canonical bytes */
    for (int v = 0; v < V_COUNT; v++) {
        CHECK_RC(lm_dec_open(&dec, t, 0, 0, 0), MOQ_OK);
        for (size_t k = 0; k < s.count; k++) {
            o = s.oid[k];
            buf_t alt;
            const uint8_t *p = s.obj[k].b;
            size_t pn = s.obj[k].n;
            uint32_t prev = dec.d._has_reference ? dec.d._ref._present : 0;
            if (!s.raw[k] && variant((variant_t)v, t, s.obj[k].b, s.obj[k].n, s.full[k].b,
                                     s.full[k].n, prev, &alt)) {
                p = alt.b;
                pn = alt.n;
            }
            moq_locmaf_chunk_t ch;
            moq_locmaf_reason_t why;
            buf_t w;
            if (moq_locmaf_decoder_push(&dec.d, g, o, (moq_bytes_t){ p, pn }, &ch, sizeof(ch),
                                        &why) != MOQ_OK || write_chunk(t, &ch, &w) != MOQ_OK ||
                w.n != s.canon[k].n || memcmp(w.b, s.canon[k].b, w.n) != 0) {
                FAIL_AT(variant_name[v]);
                fprintf(stderr, "  reason %s\n", moq_locmaf_reason_name(why));
                break;
            }
        }
        lm_dec_close(&dec);
    }
}

int main(int argc, char **argv)
{
    uint64_t seeds = argc > 1 ? strtoull(argv[1], NULL, 10) : 400;
    uint64_t first = argc > 2 ? strtoull(argv[2], NULL, 10) : 1;
    moq_locmaf_track_t tracks[5] = { lm_track_a(), lm_track_v(), lm_track_p8(),
                                     lm_track_p0(), lm_track_p16() };
    for (uint64_t seed = first; seed < first + seeds; seed++) {
        seed_now = seed;
        lm_rng_t rng = { seed * 0x9E3779B97F4A7C15ull };
        const moq_locmaf_track_t *t = &tracks[seed % 5];
        for (uint64_t g = 0; g < 3; g++) run_group(&rng, t, g);
        if (lm_failures > 20) break;
    }
    if (lm_failures)
        fprintf(stderr, "replay: test_locmaf_roundtrip 1 <seed>\n");
    return lm_finish("locmaf_roundtrip");
}
