/*
 * The canonical CMAF writer (section 15): genBoxes wrapped in payload order,
 * then moof (mfhd, traf: tfhd, tfdt, trun, and saiz, saio, senc when the
 * chunk carries auxiliary information) and mdat, in the pinned layout of
 * sections 15.2 to 15.8. Two passes: the layout is computed and checked
 * before a byte is written.
 */
#include "locmaf_internal.h"

bool locmaf_check_mdat_len(uint64_t payload_len, moq_locmaf_reason_t *why)
{
    if (payload_len <= LM_MDAT_MAX) return true;
    lm_why(why, MOQ_LOCMAF_REASON_MDAT_TOO_LARGE);
    return false;
}

bool locmaf_check_moof_size(uint64_t moof_size)
{
    return moof_size <= (uint64_t)INT32_MAX - 8;
}

/* -- Consistency ----------------------------------------------------------------- */

static uint64_t aux_size(uint8_t iv, bool subs, uint32_t count)
{
    return iv + (subs ? 2u + 6u * (uint64_t)count : 0u);
}

/* A caller-built chunk must be what a decoder could have produced. */
static bool chunk_consistent(const moq_locmaf_track_t *t, const moq_locmaf_chunk_t *c)
{
    uint32_t n = c->sample_count;
    uint64_t p = c->mdat_payload.len;
    if (c->kind != MOQ_LOCMAF_OBJECT_FULL && c->kind != MOQ_LOCMAF_OBJECT_DELTA) return false;
    if (n > 0 && !c->samples) return false;
    if (!c->mdat_payload.data && p) return false;
    if (!locmaf_check_mdat_len(p, NULL)) return false;
    if (n == 0 && (p != 0 || c->has_aux_info)) return false;
    if (c->gen_box_count && !c->gen_boxes) return false;
    for (size_t k = 0; k < c->gen_box_count; k++) {
        const moq_locmaf_gen_box_t *g = &c->gen_boxes[k];
        if ((!g->payload.data && g->payload.len) || g->payload.len > UINT32_MAX - 8u)
            return false;
    }

    uint8_t iv = c->per_sample_iv_size;
    bool aux = c->has_aux_info, subs = c->has_subsamples;
    if (iv != 0 && iv != 8 && iv != 16) return false;
    if (subs && !aux) return false;
    if (aux && (!t->is_protected || (iv == 0 && !subs))) return false;
    /* Sections 13.1, 15.8: a protected chunk with a per-sample IV size above
     * zero carries its IVs, so its senc, saiz and saio are written. */
    if (t->is_protected && iv > 0 && n > 0 && !aux) return false;
    size_t iv_bytes = aux ? (size_t)n * iv : 0;
    if (c->ivs.len != iv_bytes || (iv_bytes && !c->ivs.data)) return false;
    if (c->subsample_total && !c->subsamples) return false;

    uint64_t off = 0, sub = 0;
    bool negative = false, wide = false;
    for (uint32_t i = 0; i < n; i++) {
        const moq_locmaf_sample_t *s = &c->samples[i];
        if (s->offset != off) return false;
        off += s->size;                            /* n * 2^32 fits */
        if (s->composition_offset < INT32_MIN || s->composition_offset > (int64_t)UINT32_MAX)
            return false;
        negative |= s->composition_offset < 0;
        wide |= s->composition_offset > INT32_MAX;
        if (s->subsample_count > 65535 || (!subs && s->subsample_count)) return false;
        if (s->subsample_index != sub) return false;
        if (s->subsample_count > c->subsample_total - sub) return false;
        if (s->subsample_count) {
            uint64_t bytes = 0;
            for (uint32_t k = 0; k < s->subsample_count; k++) {
                const moq_locmaf_subsample_t *e = &c->subsamples[sub + k];
                if (e->clear_bytes > 65535) return false;
                bytes += (uint64_t)e->clear_bytes + e->protected_bytes;
            }
            if (bytes != s->size) return false;
        }
        if (aux && aux_size(iv, subs, s->subsample_count) > 255) return false;
        sub += s->subsample_count;
    }
    if (off != p || sub != c->subsample_total || (negative && wide)) return false;
    return true;
}

/* -- Layout ------------------------------------------------------------------------ */

typedef struct layout {
    locmaf_canon_t k;
    uint32_t tfhd_flags, trun_flags;
    uint8_t  trun_version;
    size_t   tfhd, trun, saiz, senc, moof, gen_bytes, total;
    bool     aux_same;
    uint8_t  aux_default;
} layout_t;

static bool add(size_t *acc, uint64_t x)
{
    if (x > SIZE_MAX) return false;
    return lm_size_add(*acc, (size_t)x, acc);
}

static bool layout_of(const moq_locmaf_track_t *t, const moq_locmaf_chunk_t *c, layout_t *l)
{
    memset(l, 0, sizeof(*l));
    uint32_t n = c->sample_count;
    locmaf_decide(t, c->sample_description_index, c->samples, n, c->mdat_payload.len, &l->k);
    const locmaf_canon_t *k = &l->k;

    l->tfhd_flags = 0x020000u | (k->sdi ? 0x02u : 0) | (k->dur_default ? 0x08u : 0) |
                    (k->size_box ? 0x10u : 0) | (k->flags_default ? 0x20u : 0);
    l->tfhd = 16u + 4u * (size_t)(k->sdi + k->dur_default + k->size_box + k->flags_default);
    l->trun_flags = 0x001u | (k->first_flags ? 0x004u : 0) | (k->dur_list ? 0x100u : 0) |
                    (k->size_list ? 0x200u : 0) | (k->flags_list ? 0x400u : 0) |
                    (k->cto ? 0x800u : 0);
    l->trun_version = k->cto_signed ? 1 : 0;
    uint64_t per = 4u * (uint64_t)(k->dur_list + k->size_list + k->flags_list + k->cto);
    uint64_t trun = 20u + (k->first_flags ? 4u : 0u) + per * n;    /* < 2^39 */

    uint64_t moof = 32u + l->tfhd + 20u + trun;
    if (c->has_aux_info) {
        l->aux_same = true;
        uint64_t sum = 0;
        for (uint32_t i = 0; i < n; i++) {
            uint64_t a = aux_size(c->per_sample_iv_size, c->has_subsamples,
                                  c->samples[i].subsample_count);
            if (i == 0) l->aux_default = (uint8_t)a;
            else if (a != l->aux_default) l->aux_same = false;
            sum += a;                               /* n * 255 */
        }
        uint64_t saiz = 17u + (l->aux_same ? 0u : n);
        uint64_t senc = 16u + sum;
        l->saiz = (size_t)saiz;
        l->senc = (size_t)senc;
        moof += saiz + 20u + senc;
    }
    if (!locmaf_check_moof_size(moof)) return false;
    l->trun = (size_t)trun;
    l->moof = (size_t)moof;

    size_t total = 0;
    for (size_t g = 0; g < c->gen_box_count; g++)
        if (!add(&total, 8u + (uint64_t)c->gen_boxes[g].payload.len)) return false;
    l->gen_bytes = total;
    if (!add(&total, moof) || !add(&total, 8u + (uint64_t)c->mdat_payload.len)) return false;
    l->total = total;
    return true;
}

/* -- Emit -------------------------------------------------------------------------- */

typedef struct emitter { uint8_t *p; } emitter_t;

static void e_u8(emitter_t *e, uint8_t v) { *e->p++ = v; }
static void e_u16(emitter_t *e, uint16_t v) { lm_wr16(e->p, v); e->p += 2; }
static void e_u32(emitter_t *e, uint32_t v) { lm_wr32(e->p, v); e->p += 4; }
static void e_u64(emitter_t *e, uint64_t v) { lm_wr64(e->p, v); e->p += 8; }
static void e_raw(emitter_t *e, const uint8_t *src, size_t n)
{
    if (n) memcpy(e->p, src, n);
    e->p += n;
}
static void e_box(emitter_t *e, size_t size, uint32_t type)
{
    e_u32(e, (uint32_t)size);
    e_u32(e, type);
}

static void emit(const moq_locmaf_track_t *t, const moq_locmaf_chunk_t *c,
                 const layout_t *l, uint8_t *out)
{
    emitter_t e = { out };
    const locmaf_canon_t *k = &l->k;
    uint32_t n = c->sample_count;

    for (size_t g = 0; g < c->gen_box_count; g++) {
        const moq_locmaf_gen_box_t *b = &c->gen_boxes[g];
        e_box(&e, 8 + b->payload.len, b->type);
        e_raw(&e, b->payload.data, b->payload.len);
    }

    e_box(&e, l->moof, LM_FOURCC('m','o','o','f'));
    e_box(&e, 16, LM_FOURCC('m','f','h','d'));
    e_u32(&e, 0);
    e_u32(&e, 0);                                   /* 15.3: sequence_number 0 */
    e_box(&e, l->moof - 24, LM_FOURCC('t','r','a','f'));

    e_box(&e, l->tfhd, LM_FOURCC('t','f','h','d'));
    e_u32(&e, l->tfhd_flags);
    e_u32(&e, t->track_id);
    if (k->sdi) e_u32(&e, c->sample_description_index);
    if (k->dur_default) e_u32(&e, k->dur);
    if (k->size_box) e_u32(&e, k->size);
    if (k->flags_default) e_u32(&e, k->flags);

    e_box(&e, 20, LM_FOURCC('t','f','d','t'));
    e_u32(&e, 0x01000000u);
    e_u64(&e, c->base_media_decode_time);

    e_box(&e, l->trun, LM_FOURCC('t','r','u','n'));
    e_u32(&e, ((uint32_t)l->trun_version << 24) | l->trun_flags);
    e_u32(&e, n);
    e_u32(&e, (uint32_t)(l->moof + 8));            /* 15.7 */
    if (k->first_flags) e_u32(&e, k->first);
    for (uint32_t i = 0; i < n; i++) {
        const moq_locmaf_sample_t *s = &c->samples[i];
        if (k->dur_list) e_u32(&e, s->duration);
        if (k->size_list) e_u32(&e, s->size);
        if (k->flags_list) e_u32(&e, s->flags);
        /* trun v1 holds int32, v0 uint32: the range was checked. */
        if (k->cto) e_u32(&e, (uint32_t)(uint64_t)s->composition_offset);
    }

    if (c->has_aux_info) {
        size_t saiz_at = 52 + l->tfhd + l->trun;     /* from the moof start */
        e_box(&e, l->saiz, LM_FOURCC('s','a','i','z'));
        e_u32(&e, 0);
        e_u8(&e, l->aux_same ? l->aux_default : 0);
        e_u32(&e, n);
        for (uint32_t i = 0; !l->aux_same && i < n; i++)
            e_u8(&e, (uint8_t)aux_size(c->per_sample_iv_size, c->has_subsamples,
                                       c->samples[i].subsample_count));
        e_box(&e, 20, LM_FOURCC('s','a','i','o'));
        e_u32(&e, 0);
        e_u32(&e, 1);
        e_u32(&e, (uint32_t)(saiz_at + l->saiz + 20 + 16));   /* 15.8 */
        e_box(&e, l->senc, LM_FOURCC('s','e','n','c'));
        e_u32(&e, c->has_subsamples ? 0x000002u : 0);
        e_u32(&e, n);
        size_t iv = c->per_sample_iv_size;
        for (uint32_t i = 0; i < n; i++) {
            const moq_locmaf_sample_t *s = &c->samples[i];
            e_raw(&e, c->ivs.data ? c->ivs.data + (size_t)i * iv : NULL, iv);
            if (!c->has_subsamples) continue;
            e_u16(&e, (uint16_t)s->subsample_count);
            for (uint32_t j = 0; j < s->subsample_count; j++) {
                const moq_locmaf_subsample_t *x = &c->subsamples[s->subsample_index + j];
                e_u16(&e, (uint16_t)x->clear_bytes);
                e_u32(&e, x->protected_bytes);
            }
        }
    }

    e_box(&e, 8 + c->mdat_payload.len, LM_FOURCC('m','d','a','t'));
    e_raw(&e, c->mdat_payload.data, c->mdat_payload.len);
}

moq_result_t moq_locmaf_write_cmaf(const moq_locmaf_track_t *track,
                                   const moq_locmaf_chunk_t *chunk,
                                   uint8_t *out, size_t out_cap, size_t *out_len)
{
    if (!track || !chunk || !out_len || (!out && out_cap)) return MOQ_ERR_INVAL;
    moq_locmaf_track_t t;
    if (locmaf_track_read(track, &t) != MOQ_OK) return MOQ_ERR_INVAL;
    uint32_t size;
    memcpy(&size, &chunk->struct_size, sizeof(size));
    if (size < MOQ_LOCMAF_CHUNK_V0_SIZE) return MOQ_ERR_INVAL;
    moq_locmaf_chunk_t c;
    memset(&c, 0, sizeof(c));
    memcpy(&c, chunk, MOQ_LOCMAF_CHUNK_V0_SIZE);

    if (c.kind == MOQ_LOCMAF_OBJECT_RAW_BOXES) {
        /* Section 9.4: the boxes, verbatim -- once they pass the checks a
         * decoded rawBoxes Object passed (sections 9.1, 18). */
        if (!c.raw_boxes.data || !c.raw_boxes.len || !locmaf_raw_boxes_ok(c.raw_boxes) ||
            locmaf_raw_chunk_check(&t, c.raw_boxes, NULL) != MOQ_OK)
            return MOQ_ERR_INVAL;
        *out_len = c.raw_boxes.len;
        if (out_cap < c.raw_boxes.len) return MOQ_ERR_BUFFER;
        memcpy(out, c.raw_boxes.data, c.raw_boxes.len);
        return MOQ_OK;
    }
    if (!chunk_consistent(&t, &c)) return MOQ_ERR_INVAL;
    layout_t l;
    if (!layout_of(&t, &c, &l)) return MOQ_ERR_INVAL;
    *out_len = l.total;
    if (out_cap < l.total) return MOQ_ERR_BUFFER;
    emit(&t, &c, &l, out);
    return MOQ_OK;
}
