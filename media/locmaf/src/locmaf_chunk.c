/*
 * The lossless CMAF chunk reader: the encoder's input. One chunk -- the
 * boxes before its moof, one moof and one mdat -- read into the effective
 * values of section 15.1 with the trex fallback, and refused when it leaves
 * the field model of section 10 (sections 4.1, 13.2) or breaks its boxes.
 */
#include "locmaf_internal.h"

typedef struct rd_box {
    uint32_t       type;
    const uint8_t *start;    /* the box header */
    const uint8_t *body;
    size_t         len;      /* body bytes */
    size_t         size;
} rd_box_t;

static moq_result_t fail(moq_locmaf_reason_t *why, moq_locmaf_reason_t r)
{
    lm_why(why, r);
    switch (r) {
    case MOQ_LOCMAF_REASON_MALFORMED_CMAF:
    case MOQ_LOCMAF_REASON_TRACK_ID:
    case MOQ_LOCMAF_REASON_CLEAR_TRACK_CENC:
        return MOQ_ERR_PROTO;
    case MOQ_LOCMAF_REASON_LIMIT:
        return MOQ_ERR_LIMIT;
    default:
        return MOQ_ERR_UNSUPPORTED;
    }
}

/* One box at p: an escape size is outside the model, a short or overrunning
 * one is malformed. */
static moq_result_t next_box(const uint8_t *p, size_t avail, rd_box_t *b,
                             moq_locmaf_reason_t *why)
{
    if (avail < 8) return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
    uint32_t size = lm_rd32(p);
    if (size == 0 || size == 1) return fail(why, MOQ_LOCMAF_REASON_SIZE_ESCAPE);
    if (size < 8 || size > avail) return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
    b->type = lm_rd32(p + 4);
    b->start = p;
    b->body = p + 8;
    b->len = (size_t)size - 8;
    b->size = size;
    return MOQ_OK;
}

static bool fullbox(const rd_box_t *b, uint8_t *ver, uint32_t *flags)
{
    if (b->len < 4) return false;
    *ver = b->body[0];
    *flags = ((uint32_t)b->body[1] << 16) | ((uint32_t)b->body[2] << 8) | b->body[3];
    return true;
}

typedef struct traf_boxes {
    rd_box_t tfhd, tfdt, trun, senc, saiz, saio;
    bool     has_tfhd, has_tfdt, has_trun, has_senc, has_saiz, has_saio;
} traf_boxes_t;

static moq_result_t read_traf(const rd_box_t *traf, traf_boxes_t *tb,
                              moq_locmaf_reason_t *why)
{
    memset(tb, 0, sizeof(*tb));
    size_t pos = 0;
    while (pos < traf->len) {
        rd_box_t b;
        moq_result_t rc = next_box(traf->body + pos, traf->len - pos, &b, why);
        if (rc != MOQ_OK) return rc;
        pos += b.size;
        rd_box_t *slot = NULL;
        bool *seen = NULL;
        switch (b.type) {
        case LM_FOURCC('t','f','h','d'): slot = &tb->tfhd; seen = &tb->has_tfhd; break;
        case LM_FOURCC('t','f','d','t'): slot = &tb->tfdt; seen = &tb->has_tfdt; break;
        case LM_FOURCC('t','r','u','n'): slot = &tb->trun; seen = &tb->has_trun; break;
        case LM_FOURCC('s','e','n','c'): slot = &tb->senc; seen = &tb->has_senc; break;
        case LM_FOURCC('s','a','i','z'): slot = &tb->saiz; seen = &tb->has_saiz; break;
        case LM_FOURCC('s','a','i','o'): slot = &tb->saio; seen = &tb->has_saio; break;
        case LM_FOURCC('s','b','g','p'):
        case LM_FOURCC('s','g','p','d'): return fail(why, MOQ_LOCMAF_REASON_SAMPLE_GROUPS);
        case LM_FOURCC('s','u','b','s'): return fail(why, MOQ_LOCMAF_REASON_SUBS);
        default: return fail(why, MOQ_LOCMAF_REASON_UNKNOWN_BOX);
        }
        if (*seen) return fail(why, MOQ_LOCMAF_REASON_STRUCTURE);
        *seen = true;
        *slot = b;
    }
    if (!tb->has_tfhd || !tb->has_tfdt || !tb->has_trun)
        return fail(why, MOQ_LOCMAF_REASON_STRUCTURE);
    return MOQ_OK;
}

typedef struct tfhd_vals {
    uint32_t flags, sdi, dur, size, sflags;
} tfhd_vals_t;

static moq_result_t read_tfhd(const rd_box_t *b, const moq_locmaf_track_t *t,
                              tfhd_vals_t *v, moq_locmaf_reason_t *why)
{
    uint8_t ver = 0;
    uint32_t fl = 0;
    if (!fullbox(b, &ver, &fl) || ver != 0) return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
    const uint32_t known = 0x01 | 0x02 | 0x08 | 0x10 | 0x20 | 0x010000 | 0x020000;
    if (fl & ~known) return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
    size_t want = 8 + ((fl & 0x01) ? 8u : 0u) + ((fl & 0x02) ? 4u : 0u) +
                  ((fl & 0x08) ? 4u : 0u) + ((fl & 0x10) ? 4u : 0u) + ((fl & 0x20) ? 4u : 0u);
    if (b->len != want) return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
    if (fl & 0x01) return fail(why, MOQ_LOCMAF_REASON_DATA_LAYOUT);
    if (fl & 0x010000) return fail(why, MOQ_LOCMAF_REASON_STRUCTURE);
    if (lm_rd32(b->body + 4) != t->track_id) return fail(why, MOQ_LOCMAF_REASON_TRACK_ID);
    const uint8_t *p = b->body + 8;
    v->flags = fl;
    v->sdi = t->trex_default_sample_description_index;
    v->dur = t->trex_default_sample_duration;
    v->size = t->trex_default_sample_size;
    v->sflags = t->trex_default_sample_flags;
    if (fl & 0x02) { v->sdi = lm_rd32(p); p += 4; }
    if (fl & 0x08) { v->dur = lm_rd32(p); p += 4; }
    if (fl & 0x10) { v->size = lm_rd32(p); p += 4; }
    if (fl & 0x20) { v->sflags = lm_rd32(p); }
    return MOQ_OK;
}

static moq_result_t read_trun(const rd_box_t *b, const tfhd_vals_t *def, size_t moof_size,
                              const locmaf_limits_t *lim, moq_locmaf_sample_t *s,
                              uint32_t *n_out, moq_locmaf_reason_t *why)
{
    uint8_t ver = 0;
    uint32_t fl = 0;
    if (!fullbox(b, &ver, &fl) || ver > 1 || b->len < 8)
        return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
    const uint32_t known = 0x001 | 0x004 | 0x100 | 0x200 | 0x400 | 0x800;
    if ((fl & ~known) || ((fl & 0x004) && (fl & 0x400)))
        return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
    uint64_t n = lm_rd32(b->body + 4);
    uint64_t fixed = 8 + ((fl & 0x001) ? 4u : 0u) + ((fl & 0x004) ? 4u : 0u);
    uint64_t per = 4u * (uint64_t)(((fl & 0x100) != 0) + ((fl & 0x200) != 0) +
                                   ((fl & 0x400) != 0) + ((fl & 0x800) != 0));
    if (b->len < fixed || (per ? n > (b->len - fixed) / per || n * per != b->len - fixed
                               : b->len != fixed))
        return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
    if (n > lim->samples) return fail(why, MOQ_LOCMAF_REASON_LIMIT);
    if (!(fl & 0x001)) return fail(why, MOQ_LOCMAF_REASON_DATA_LAYOUT);
    const uint8_t *p = b->body + 8;
    if (lm_rd32(p) != (uint64_t)moof_size + 8) return fail(why, MOQ_LOCMAF_REASON_DATA_LAYOUT);
    p += 4;
    uint32_t first = 0;
    if (fl & 0x004) { first = lm_rd32(p); p += 4; }
    for (uint32_t i = 0; i < n; i++) {
        s[i].duration = def->dur;
        s[i].size = def->size;
        s[i].flags = (i == 0 && (fl & 0x004)) ? first : def->sflags;
        s[i].composition_offset = 0;
        if (fl & 0x100) { s[i].duration = lm_rd32(p); p += 4; }
        if (fl & 0x200) { s[i].size = lm_rd32(p); p += 4; }
        if (fl & 0x400) { s[i].flags = lm_rd32(p); p += 4; }
        if (fl & 0x800) {
            uint32_t raw = lm_rd32(p);
            p += 4;
            /* v0 offsets are unsigned, v1 signed (ISO/IEC 14496-12 8.8.8). */
            s[i].composition_offset = ver == 0 ? (int64_t)raw
                                    : (int64_t)(raw > INT32_MAX ? (int64_t)raw - 4294967296LL
                                                                : (int64_t)raw);
        }
        s[i].subsample_count = 0;
        s[i].subsample_index = 0;
        s[i]._reserved = 0;
    }
    *n_out = (uint32_t)n;
    return MOQ_OK;
}

static uint64_t aux_of(uint8_t iv, bool subs, uint32_t count)
{
    return iv + (subs ? 2u + 6u * (uint64_t)count : 0u);
}

static moq_result_t read_senc(const rd_box_t *b, const moq_locmaf_track_t *t,
                              const locmaf_limits_t *lim, locmaf_src_t *src,
                              const locmaf_scratch_t *scr, moq_locmaf_reason_t *why)
{
    if (!t->is_protected) return fail(why, MOQ_LOCMAF_REASON_CLEAR_TRACK_CENC);
    uint8_t ver = 0;
    uint32_t fl = 0;
    if (!fullbox(b, &ver, &fl) || ver != 0 || (fl & ~0x2u))
        return fail(why, MOQ_LOCMAF_REASON_AUX_INFO);
    if (b->len < 8 || lm_rd32(b->body + 4) != src->n)
        return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
    bool subs = (fl & 0x2) != 0;
    uint8_t iv = t->default_per_sample_iv_size;
    size_t pos = 8, total = 0;
    for (uint32_t i = 0; i < src->n; i++) {
        if (b->len - pos < iv) return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
        if (iv) memcpy(scr->ivs + (size_t)i * iv, b->body + pos, iv);
        pos += iv;
        moq_locmaf_sample_t *s = &scr->samples[i];
        s->subsample_index = (uint32_t)total;
        if (!subs) continue;
        if (b->len - pos < 2) return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
        uint32_t c = lm_rd16(b->body + pos);
        pos += 2;
        if (c > (b->len - pos) / 6) return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
        if (c > lim->subsamples - total) return fail(why, MOQ_LOCMAF_REASON_LIMIT);
        uint64_t bytes = 0;
        for (uint32_t k = 0; k < c; k++, pos += 6) {
            moq_locmaf_subsample_t *e = &scr->subsamples[total + k];
            e->clear_bytes = lm_rd16(b->body + pos);
            e->protected_bytes = lm_rd32(b->body + pos + 2);
            bytes += (uint64_t)e->clear_bytes + e->protected_bytes;
        }
        /* Section 18: the map covers the sample exactly; the aux info fits
         * the 8-bit saiz entry. */
        if (c > 0 && bytes != s->size) return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
        if (aux_of(iv, true, c) > 255) return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
        s->subsample_count = c;
        total += c;
    }
    if (pos != b->len) return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
    src->iv = iv;
    src->has_subsamples = subs && src->n > 0;
    src->has_aux_info = src->n > 0 && (subs || iv > 0);
    src->subsample_total = total;
    if (!src->has_aux_info) src->has_subsamples = false;
    return MOQ_OK;
}

/* saiz and saio, when present, must describe the senc as section 15.8
 * rebuilds it. */
static moq_result_t check_saiz_saio(const traf_boxes_t *tb, const uint8_t *moof,
                                    const locmaf_src_t *src, const locmaf_scratch_t *scr,
                                    moq_locmaf_reason_t *why)
{
    if (!tb->has_saiz && !tb->has_saio) return MOQ_OK;
    if (!tb->has_senc || !tb->has_saiz || !tb->has_saio)
        return fail(why, MOQ_LOCMAF_REASON_AUX_INFO);
    uint8_t ver = 0;
    uint32_t fl = 0;
    const rd_box_t *z = &tb->saiz;
    if (!fullbox(z, &ver, &fl) || ver != 0 || (fl & ~0x1u))
        return fail(why, MOQ_LOCMAF_REASON_AUX_INFO);
    size_t pos = 4 + ((fl & 0x1) ? 8u : 0u);
    if (z->len < pos + 5) return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
    uint8_t def = z->body[pos];
    uint32_t count = lm_rd32(z->body + pos + 1);
    pos += 5;
    if (count != src->n || z->len - pos != (def ? 0u : (size_t)count))
        return fail(why, MOQ_LOCMAF_REASON_AUX_INFO);
    for (uint32_t i = 0; i < src->n; i++) {
        uint64_t a = aux_of(src->iv, src->has_subsamples, scr->samples[i].subsample_count);
        if ((def ? def : z->body[pos + i]) != a) return fail(why, MOQ_LOCMAF_REASON_AUX_INFO);
    }
    const rd_box_t *o = &tb->saio;
    if (!fullbox(o, &ver, &fl) || ver > 1 || (fl & ~0x1u))
        return fail(why, MOQ_LOCMAF_REASON_AUX_INFO);
    pos = 4 + ((fl & 0x1) ? 8u : 0u);
    size_t width = ver ? 8u : 4u;
    if (o->len != pos + 4 + width || lm_rd32(o->body + pos) != 1)
        return fail(why, MOQ_LOCMAF_REASON_AUX_INFO);
    uint64_t off = ver ? lm_rd64(o->body + pos + 4) : lm_rd32(o->body + pos + 4);
    if (off != (uint64_t)(tb->senc.start - moof) + 16)
        return fail(why, MOQ_LOCMAF_REASON_AUX_INFO);
    return MOQ_OK;
}

static moq_result_t read_moof(const rd_box_t *moof, const moq_locmaf_track_t *t,
                              const locmaf_limits_t *lim, locmaf_src_t *src,
                              const locmaf_scratch_t *scr, moq_locmaf_reason_t *why)
{
    rd_box_t traf;
    memset(&traf, 0, sizeof(traf));
    bool has_mfhd = false;
    size_t trafs = 0, pos = 0;
    while (pos < moof->len) {
        rd_box_t b;
        moq_result_t rc = next_box(moof->body + pos, moof->len - pos, &b, why);
        if (rc != MOQ_OK) return rc;
        pos += b.size;
        if (b.type == LM_FOURCC('m','f','h','d')) {
            uint8_t ver = 0;
            uint32_t fl = 0;
            if (has_mfhd || !fullbox(&b, &ver, &fl) || ver != 0 || b.len != 8)
                return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
            has_mfhd = true;
        } else if (b.type == LM_FOURCC('t','r','a','f')) {
            traf = b;
            trafs++;
        } else if (b.type == LM_FOURCC('p','s','s','h')) {
            return fail(why, MOQ_LOCMAF_REASON_FRAGMENT_PSSH);
        } else {
            return fail(why, MOQ_LOCMAF_REASON_UNKNOWN_BOX);
        }
    }
    if (!has_mfhd) return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
    if (trafs != 1) return fail(why, MOQ_LOCMAF_REASON_STRUCTURE);

    traf_boxes_t tb;
    moq_result_t rc = read_traf(&traf, &tb, why);
    if (rc != MOQ_OK) return rc;
    tfhd_vals_t def;
    rc = read_tfhd(&tb.tfhd, t, &def, why);
    if (rc != MOQ_OK) return rc;
    src->sdi = def.sdi;
    uint8_t ver = 0;
    uint32_t fl = 0;
    if (!fullbox(&tb.tfdt, &ver, &fl) || ver > 1 || tb.tfdt.len != (ver ? 12u : 8u))
        return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
    src->bmdt = ver ? lm_rd64(tb.tfdt.body + 4) : lm_rd32(tb.tfdt.body + 4);
    rc = read_trun(&tb.trun, &def, moof->size, lim, scr->samples, &src->n, why);
    if (rc != MOQ_OK) return rc;
    if (tb.has_senc) {
        rc = read_senc(&tb.senc, t, lim, src, scr, why);
        if (rc != MOQ_OK) return rc;
    } else if (t->is_protected && t->default_per_sample_iv_size > 0 && src->n > 0) {
        /* The samples' IVs are not in a senc this reader can carry. */
        return fail(why, MOQ_LOCMAF_REASON_AUX_INFO);
    }
    return check_saiz_saio(&tb, moof->start, src, scr, why);
}

moq_result_t locmaf_read_chunk(const moq_locmaf_track_t *track,
                               moq_bytes_t bytes, const locmaf_limits_t *lim,
                               locmaf_src_t *src, const locmaf_scratch_t *scr,
                               moq_locmaf_reason_t *why)
{
    memset(src, 0, sizeof(*src));
    size_t pos = 0, count = 0;
    bool styp = false, prft = false, moof_done = false, mdat_done = false;
    bool stray = false;
    rd_box_t moof;
    memset(&moof, 0, sizeof(moof));
    while (pos < bytes.len) {
        rd_box_t b;
        moq_result_t rc = next_box(bytes.data + pos, bytes.len - pos, &b, why);
        if (rc != MOQ_OK) return rc;
        pos += b.size;
        if (mdat_done) {
            /* Anything after the chunk: a second chunk, or stray boxes. */
            if (b.type == LM_FOURCC('m','o','o','f')) return fail(why, MOQ_LOCMAF_REASON_MULTI_CHUNK);
            stray = true;
            continue;
        }
        if (moof_done) {
            if (b.type != LM_FOURCC('m','d','a','t')) return fail(why, MOQ_LOCMAF_REASON_STRUCTURE);
            src->mdat_payload = (moq_bytes_t){ b.len ? b.body : NULL, b.len };
            mdat_done = true;
            continue;
        }
        if (b.type == LM_FOURCC('m','o','o','f')) {
            moof = b;
            moof_done = true;
            rc = read_moof(&moof, track, lim, src, scr, why);
            if (rc != MOQ_OK) return rc;
            continue;
        }
        if (!locmaf_gen_box_type_ok(b.type)) return fail(why, MOQ_LOCMAF_REASON_STRUCTURE);
        bool *seen = b.type == LM_FOURCC('s','t','y','p') ? &styp
                   : b.type == LM_FOURCC('p','r','f','t') ? &prft : NULL;
        if (seen && *seen) return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
        if (seen) *seen = true;
        moq_bytes_t body = { b.len ? b.body : NULL, b.len };
        moq_locmaf_reason_t r = MOQ_LOCMAF_REASON_NONE;
        rc = locmaf_gen_box_check(b.type, body, &r);
        if (rc == MOQ_ERR_UNSUPPORTED) return fail(why, MOQ_LOCMAF_REASON_GEN_BOX_UNSUPPORTED);
        if (rc != MOQ_OK) return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
        if (count == lim->gen_boxes) return fail(why, MOQ_LOCMAF_REASON_LIMIT);
        scr->gen_boxes[count].type = b.type;
        scr->gen_boxes[count]._reserved = 0;
        scr->gen_boxes[count].payload = body;
        count++;
    }
    if (!moof_done || !mdat_done || stray) return fail(why, MOQ_LOCMAF_REASON_STRUCTURE);
    src->gen_box_count = count;

    /* The samples fill the mdat payload exactly, in order. */
    uint64_t p = src->mdat_payload.len, off = 0;
    for (uint32_t i = 0; i < src->n; i++) {
        scr->samples[i].offset = off;
        off += scr->samples[i].size;               /* n * 2^32 fits */
    }
    if (off > p) return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
    if (off < p) return fail(why, MOQ_LOCMAF_REASON_DATA_LAYOUT);
    /* A list with a negative composition offset must fit trun version 1
     * throughout, as the decoder requires. */
    for (uint32_t i = 0; i < src->n; i++)
        if (scr->samples[i].composition_offset < 0) {
            for (uint32_t k = 0; k < src->n; k++)
                if (scr->samples[k].composition_offset > INT32_MAX)
                    return fail(why, MOQ_LOCMAF_REASON_MALFORMED_CMAF);
            break;
        }
    return MOQ_OK;
}

bool locmaf_encode_bound_counts(uint64_t gen_box_bytes, size_t gen_box_count,
                                uint64_t n, uint64_t subsamples,
                                uint64_t payload_len, size_t *out)
{
    /* The longest Object a chunk can encode to, full or delta: each genBox
     * at most 1 + 5 bytes of element header (and 4 + payload, counted by the
     * caller); the header element 1 + 9; the fields, each with its ID: two
     * of up to 9 bytes, seven of up to 5, and the length prefixes of the
     * lists; per sample, five list elements of up to 5 bytes (no element
     * delta exceeds 2^34 in zigzag form), 16 IV bytes and a 3-byte subsample
     * count; 8 bytes per subsample entry; the deletion list; the payload. */
    uint64_t b = 0;
    if (!locmaf_u64_add(b, gen_box_bytes, &b)) return false;
    if (gen_box_count > UINT64_MAX / 10 ||
        !locmaf_u64_add(b, (uint64_t)gen_box_count * 10, &b))
        return false;
    uint64_t fixed = 10 + 2 * (1 + 9) + 7 * (1 + 5) + 7 * (1 + 9) + (1 + 1 + 17);
    if (!locmaf_u64_add(b, fixed, &b)) return false;
    if (n > UINT64_MAX / 44 || !locmaf_u64_add(b, n * (25 + 16 + 3), &b)) return false;
    if (subsamples > UINT64_MAX / 8 || !locmaf_u64_add(b, subsamples * 8, &b)) return false;
    if (!locmaf_u64_add(b, payload_len, &b)) return false;
    if (b > SIZE_MAX) return false;
    *out = (size_t)b;
    return true;
}

bool locmaf_encode_bound(const locmaf_src_t *src, const moq_locmaf_gen_box_t *gen_boxes,
                         size_t *out)
{
    uint64_t gb = 0;
    for (size_t k = 0; k < src->gen_box_count; k++) {
        /* box_size = 4 + payload, carried after its 6-byte element header */
        if (!locmaf_u64_add(gb, gen_boxes[k].payload.len, &gb) ||
            !locmaf_u64_add(gb, 4, &gb))
            return false;
    }
    return locmaf_encode_bound_counts(gb, src->gen_box_count, src->n,
                                      src->subsample_total, src->mdat_payload.len, out);
}
