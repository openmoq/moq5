/*
 * LOCMAF wire: the element split of an Object (section 7.1), property
 * framing (7.2, 7.3), the structural box validators of section 18, the
 * tuple index of a header, and the stateless public entry points.
 */
#include "locmaf_internal.h"

/* -- Names ---------------------------------------------------------------- */

const char *moq_locmaf_reason_name(moq_locmaf_reason_t reason)
{
    switch (reason) {
    case MOQ_LOCMAF_REASON_NONE:                  return "none";
    case MOQ_LOCMAF_REASON_EMPTY_OBJECT:          return "empty_object";
    case MOQ_LOCMAF_REASON_UNKNOWN_ELEMENT:       return "unknown_element";
    case MOQ_LOCMAF_REASON_RAW_BOXES_NOT_FIRST:   return "raw_boxes_not_first";
    case MOQ_LOCMAF_REASON_TRUNCATED:             return "truncated";
    case MOQ_LOCMAF_REASON_GEN_BOX_SIZE:          return "gen_box_size";
    case MOQ_LOCMAF_REASON_GEN_BOX_TYPE:          return "gen_box_type";
    case MOQ_LOCMAF_REASON_NO_HEADER:             return "no_header";
    case MOQ_LOCMAF_REASON_RAW_BOXES_MALFORMED:   return "raw_boxes_malformed";
    case MOQ_LOCMAF_REASON_PROPERTY_FRAMING:      return "property_framing";
    case MOQ_LOCMAF_REASON_DUPLICATE_FIELD:       return "duplicate_field";
    case MOQ_LOCMAF_REASON_FIELD_KIND:            return "field_kind";
    case MOQ_LOCMAF_REASON_MISSING_FIELD:         return "missing_field";
    case MOQ_LOCMAF_REASON_LIST_LENGTH:           return "list_length";
    case MOQ_LOCMAF_REASON_VALUE_RANGE:           return "value_range";
    case MOQ_LOCMAF_REASON_DELETION:              return "deletion";
    case MOQ_LOCMAF_REASON_SAMPLE_SIZES:          return "sample_sizes";
    case MOQ_LOCMAF_REASON_CLEAR_TRACK_CENC:      return "clear_track_cenc";
    case MOQ_LOCMAF_REASON_IV_LENGTH:             return "iv_length";
    case MOQ_LOCMAF_REASON_CENC_LAYOUT:           return "cenc_layout";
    case MOQ_LOCMAF_REASON_SUBSAMPLE_SUM:         return "subsample_sum";
    case MOQ_LOCMAF_REASON_AUX_INFO_SIZE:         return "aux_info_size";
    case MOQ_LOCMAF_REASON_MDAT_TOO_LARGE:        return "mdat_too_large";
    case MOQ_LOCMAF_REASON_DELTA_AFTER_RAW_BOXES: return "delta_after_raw_boxes";
    case MOQ_LOCMAF_REASON_NO_REFERENCE:          return "no_reference";
    case MOQ_LOCMAF_REASON_LIMIT:                 return "limit";
    case MOQ_LOCMAF_REASON_GEN_BOX_REPEAT:        return "gen_box_repeat";
    case MOQ_LOCMAF_REASON_GEN_BOX_STRUCTURE:     return "gen_box_structure";
    case MOQ_LOCMAF_REASON_GEN_BOX_UNSUPPORTED:   return "gen_box_unsupported";
    case MOQ_LOCMAF_REASON_RAW_BOXES_UNSUPPORTED: return "raw_boxes_unsupported";
    case MOQ_LOCMAF_REASON_MALFORMED_CMAF:        return "malformed_cmaf";
    case MOQ_LOCMAF_REASON_TRACK_ID:              return "track_id";
    case MOQ_LOCMAF_REASON_MULTI_CHUNK:           return "multi_chunk";
    case MOQ_LOCMAF_REASON_STRUCTURE:             return "structure";
    case MOQ_LOCMAF_REASON_DATA_LAYOUT:           return "data_layout";
    case MOQ_LOCMAF_REASON_SAMPLE_GROUPS:         return "sample_groups";
    case MOQ_LOCMAF_REASON_FRAGMENT_PSSH:         return "fragment_pssh";
    case MOQ_LOCMAF_REASON_SUBS:                  return "subs";
    case MOQ_LOCMAF_REASON_AUX_INFO:              return "aux_info";
    case MOQ_LOCMAF_REASON_UNKNOWN_BOX:           return "unknown_box";
    case MOQ_LOCMAF_REASON_SIZE_ESCAPE:           return "size_escape";
    default:                                      return "";
    }
}

/* -- Track facts ------------------------------------------------------------ */

void moq_locmaf_track_init_sized(moq_locmaf_track_t *track, size_t track_size)
{
    if (!track || track_size < MOQ_LOCMAF_TRACK_V0_SIZE) return;
    memset(track, 0, sizeof(*track));
    track->struct_size = (uint32_t)sizeof(*track);
}

moq_result_t locmaf_track_read(const moq_locmaf_track_t *track,
                               moq_locmaf_track_t *out)
{
    if (!track) return MOQ_ERR_INVAL;
    uint32_t size;
    memcpy(&size, &track->struct_size, sizeof(size));
    if (size < MOQ_LOCMAF_TRACK_V0_SIZE) return MOQ_ERR_INVAL;
    moq_locmaf_track_t t;
    memcpy(&t, track, MOQ_LOCMAF_TRACK_V0_SIZE);
    if (t.default_per_sample_iv_size != 0 && t.default_per_sample_iv_size != 8 &&
        t.default_per_sample_iv_size != 16)
        return MOQ_ERR_INVAL;
    t.struct_size = (uint32_t)sizeof(t);
    t._reserved[0] = 0;
    t._reserved[1] = 0;
    *out = t;
    return MOQ_OK;
}

moq_result_t moq_locmaf_track_from_cmaf(const moq_cmaf_init_info_t *init,
                                        moq_locmaf_track_t *out,
                                        size_t out_size)
{
    if (!init || !out || out_size < MOQ_LOCMAF_TRACK_V0_SIZE) return MOQ_ERR_INVAL;
    uint32_t stamp;
    memcpy(&stamp, &init->struct_size, sizeof(stamp));
    if (stamp < MOQ_CMAF_INIT_INFO_SIZE_THROUGH_CONSTANT_IV) return MOQ_ERR_INVAL;
    if (init->trak_count != 1 || !init->has_trex || init->track_id == 0)
        return MOQ_ERR_PROTO;
    bool prot = init->has_cenc && init->default_is_protected == 1;
    uint8_t iv = prot ? init->default_per_sample_iv_size : 0;
    if (iv != 0 && iv != 8 && iv != 16) return MOQ_ERR_PROTO;

    moq_locmaf_track_t t;
    memset(&t, 0, sizeof(t));
    t.struct_size = (uint32_t)sizeof(t);
    t.track_id = init->track_id;
    t.trex_default_sample_description_index =
        init->trex_default_sample_description_index;
    t.trex_default_sample_duration = init->trex_default_sample_duration;
    t.trex_default_sample_size = init->trex_default_sample_size;
    t.trex_default_sample_flags = init->trex_default_sample_flags;
    t.is_protected = prot;
    t.default_per_sample_iv_size = iv;
    memcpy(out, &t, sizeof(t));
    return MOQ_OK;
}

moq_result_t locmaf_limits_resolve(uint32_t samples, uint32_t subsamples,
                                   uint32_t gen_boxes, locmaf_limits_t *out)
{
    if (samples > LM_CEIL_SAMPLES || subsamples > LM_CEIL_SUBSAMPLES ||
        gen_boxes > LM_CEIL_GEN_BOXES)
        return MOQ_ERR_INVAL;
    out->samples = samples ? samples : MOQ_LOCMAF_DEFAULT_MAX_SAMPLES;
    out->subsamples = subsamples ? subsamples : MOQ_LOCMAF_DEFAULT_MAX_SUBSAMPLES;
    out->gen_boxes = gen_boxes ? gen_boxes : MOQ_LOCMAF_DEFAULT_MAX_GEN_BOXES;
    return MOQ_OK;
}

/* -- Box walking ------------------------------------------------------------ */

typedef struct lm_box {
    uint32_t       type;
    const uint8_t *body;     /* contents after the 8-byte header */
    size_t         len;      /* contents length */
    size_t         size;     /* whole box */
} lm_box_t;

typedef enum {
    LM_BOX_OK = 0,
    LM_BOX_BAD,              /* size below 8 (but not an escape) or overrun */
    LM_BOX_ESCAPE,           /* size 0 or 1 */
} lm_box_rc_t;

/* One box with a plain 32-bit size at p (avail bytes). */
static lm_box_rc_t lm_box_at(const uint8_t *p, size_t avail, lm_box_t *out)
{
    if (avail < 8) return LM_BOX_BAD;
    uint32_t size = lm_rd32(p);
    if (size == 0 || size == 1) return LM_BOX_ESCAPE;
    if (size < 8 || size > avail) return LM_BOX_BAD;
    out->type = lm_rd32(p + 4);
    out->body = p + 8;
    out->len = (size_t)size - 8;
    out->size = size;
    return LM_BOX_OK;
}

/* FullBox header: version and 24-bit flags; false when shorter than 4. */
static bool lm_fullbox(const uint8_t *body, size_t len, uint8_t *ver,
                       uint32_t *flags)
{
    if (len < 4) return false;
    *ver = body[0];
    *flags = ((uint32_t)body[1] << 16) | ((uint32_t)body[2] << 8) | body[3];
    return true;
}

/* A NUL-terminated string starting at *pos inside [0, len): advances past
 * the terminator. False when the terminator is missing. */
static bool lm_cstring(const uint8_t *body, size_t len, size_t *pos)
{
    const uint8_t *nul = memchr(body + *pos, 0, len - *pos);
    if (!nul) return false;
    *pos = (size_t)(nul - body) + 1;
    return true;
}

/* -- genBox validators (section 18, the types of section 8.5's examples) ---- */

static bool check_styp(const uint8_t *b, size_t len)
{
    (void)b;
    /* 8.16.2: major_brand, minor_version, then whole compatible brands. */
    return len >= 8 && (len - 8) % 4 == 0;
}

static bool check_prft(const uint8_t *b, size_t len)
{
    uint8_t ver = 0;
    uint32_t fl = 0;
    if (!lm_fullbox(b, len, &ver, &fl)) return false;
    /* 8.16.5: reference_track_ID, ntp_timestamp (64), media_time (32/64). */
    if (ver == 0) return len == 4 + 4 + 8 + 4;
    if (ver == 1) return len == 4 + 4 + 8 + 8;
    return false;
}

static bool check_emsg(const uint8_t *b, size_t len)
{
    uint8_t ver = 0;
    uint32_t fl = 0;
    if (!lm_fullbox(b, len, &ver, &fl)) return false;
    size_t pos = 4;
    if (ver == 0) {
        /* ISO/IEC 23009-1 5.10.3.3: scheme_id_uri, value, then timescale,
         * presentation_time_delta, event_duration, id. */
        if (!lm_cstring(b, len, &pos) || !lm_cstring(b, len, &pos)) return false;
        return len - pos >= 16;
    }
    if (ver == 1) {
        /* timescale, presentation_time (64), event_duration, id, then the
         * two strings. */
        if (len - pos < 4 + 8 + 4 + 4) return false;
        pos += 20;
        return lm_cstring(b, len, &pos) && lm_cstring(b, len, &pos);
    }
    return false;
}

static bool check_sidx(const uint8_t *b, size_t len)
{
    uint8_t ver = 0;
    uint32_t fl = 0;
    if (!lm_fullbox(b, len, &ver, &fl) || ver > 1) return false;
    /* 8.16.3: reference_ID, timescale, then earliest_presentation_time and
     * first_offset (32 each in v0, 64 each in v1), reserved, count. */
    size_t fixed = 4 + 4 + 4 + (ver == 0 ? 8u : 16u) + 2 + 2;
    if (len < fixed) return false;
    uint64_t count = lm_rd16(b + fixed - 2);
    return (uint64_t)(len - fixed) == count * 12u;
}

moq_result_t locmaf_gen_box_check(uint32_t type, moq_bytes_t payload,
                                  moq_locmaf_reason_t *why)
{
    const uint8_t *b = payload.data;
    size_t len = payload.len;
    bool ok;
    switch (type) {
    case LM_FOURCC('s','t','y','p'): ok = check_styp(b, len); break;
    case LM_FOURCC('p','r','f','t'): ok = check_prft(b, len); break;
    case LM_FOURCC('e','m','s','g'): ok = check_emsg(b, len); break;
    case LM_FOURCC('s','i','d','x'): ok = check_sidx(b, len); break;
    /* 4.2: the 16-byte usertype; the rest belongs to its owner. */
    case LM_FOURCC('u','u','i','d'): ok = len >= 16; break;
    /* 8.1.2: free space has no structure. */
    case LM_FOURCC('f','r','e','e'):
    case LM_FOURCC('s','k','i','p'): ok = true; break;
    default:
        lm_why(why, MOQ_LOCMAF_REASON_GEN_BOX_UNSUPPORTED);
        return MOQ_ERR_UNSUPPORTED;
    }
    if (!ok) {
        lm_why(why, MOQ_LOCMAF_REASON_GEN_BOX_STRUCTURE);
        return MOQ_ERR_PROTO;
    }
    return MOQ_OK;
}

bool locmaf_gen_box_type_ok(uint32_t type)
{
    return type != LM_FOURCC('m','o','o','f') && type != LM_FOURCC('m','d','a','t') &&
           type != LM_FOURCC('f','t','y','p') && type != LM_FOURCC('m','o','o','v');
}

/* -- rawBoxes ---------------------------------------------------------------- */

bool locmaf_raw_boxes_ok(moq_bytes_t bytes)
{
    if (bytes.len == 0) return false;
    size_t pos = 0;
    while (pos < bytes.len) {
        lm_box_t box;
        if (lm_box_at(bytes.data + pos, bytes.len - pos, &box) != LM_BOX_OK)
            return false;
        pos += box.size;
    }
    return true;
}

/* The result of one nested validator: structure broken, or a shape this
 * library does not validate. */
typedef enum { RB_OK = 0, RB_MALFORMED, RB_UNSUPPORTED } rb_rc_t;

/* The children of a container, one box at a time (nested escapes are not
 * validated here: refused as unsupported). */
static rb_rc_t rb_child(const uint8_t *p, size_t avail, lm_box_t *out)
{
    lm_box_rc_t rc = lm_box_at(p, avail, out);
    if (rc == LM_BOX_ESCAPE) return RB_UNSUPPORTED;
    return rc == LM_BOX_OK ? RB_OK : RB_MALFORMED;
}

/* An exact count * width bytes of entries, checked before multiplying. */
static bool rb_exact(uint64_t remaining, uint64_t count, uint64_t width)
{
    if (width == 0) return remaining == 0;
    if (count > remaining / width) return false;
    return count * width == remaining;
}

static rb_rc_t rb_tfhd(const lm_box_t *bx)
{
    uint8_t ver = 0;
    uint32_t fl = 0;
    if (!lm_fullbox(bx->body, bx->len, &ver, &fl) || ver != 0) return RB_MALFORMED;
    const uint32_t known = 0x01 | 0x02 | 0x08 | 0x10 | 0x20 | 0x010000 | 0x020000;
    if (fl & ~known) return RB_MALFORMED;
    size_t want = 8 + ((fl & 0x01) ? 8u : 0u) + ((fl & 0x02) ? 4u : 0u) +
                  ((fl & 0x08) ? 4u : 0u) + ((fl & 0x10) ? 4u : 0u) +
                  ((fl & 0x20) ? 4u : 0u);
    return bx->len == want ? RB_OK : RB_MALFORMED;
}

static rb_rc_t rb_tfdt(const lm_box_t *bx)
{
    uint8_t ver = 0;
    uint32_t fl = 0;
    if (!lm_fullbox(bx->body, bx->len, &ver, &fl)) return RB_MALFORMED;
    if (ver == 0) return bx->len == 8 ? RB_OK : RB_MALFORMED;
    if (ver == 1) return bx->len == 12 ? RB_OK : RB_MALFORMED;
    return RB_MALFORMED;
}

static rb_rc_t rb_trun(const lm_box_t *bx)
{
    uint8_t ver = 0;
    uint32_t fl = 0;
    if (!lm_fullbox(bx->body, bx->len, &ver, &fl) || ver > 1 || bx->len < 8)
        return RB_MALFORMED;
    const uint32_t known = 0x001 | 0x004 | 0x100 | 0x200 | 0x400 | 0x800;
    if ((fl & ~known) || ((fl & 0x004) && (fl & 0x400))) return RB_MALFORMED;
    uint64_t count = lm_rd32(bx->body + 4);
    uint64_t fixed = 8 + ((fl & 0x001) ? 4u : 0u) + ((fl & 0x004) ? 4u : 0u);
    uint64_t per = 4u * (uint64_t)(((fl & 0x100) != 0) + ((fl & 0x200) != 0) +
                                   ((fl & 0x400) != 0) + ((fl & 0x800) != 0));
    if (bx->len < fixed) return RB_MALFORMED;
    return rb_exact(bx->len - fixed, count, per) ? RB_OK : RB_MALFORMED;
}

static rb_rc_t rb_senc_iv(const lm_box_t *bx, uint8_t iv)
{
    uint8_t ver = 0;
    uint32_t fl = 0;
    if (!lm_fullbox(bx->body, bx->len, &ver, &fl) || ver != 0 || bx->len < 8)
        return RB_MALFORMED;
    if (fl & ~0x2u) return RB_MALFORMED;
    uint64_t count = lm_rd32(bx->body + 4);
    size_t pos = 8;
    if (!(fl & 0x2)) return rb_exact(bx->len - pos, count, iv) ? RB_OK : RB_MALFORMED;
    /* Every entry takes at least its 2-byte subsample count, so the walk is
     * bounded by the box. */
    for (uint64_t i = 0; i < count; i++) {
        if (bx->len - pos < (size_t)iv + 2) return RB_MALFORMED;
        pos += iv;
        uint64_t subs = lm_rd16(bx->body + pos);
        pos += 2;
        if (subs > (bx->len - pos) / 6) return RB_MALFORMED;
        pos += (size_t)subs * 6;
    }
    return pos == bx->len ? RB_OK : RB_MALFORMED;
}

/* iv < 0: no track facts; the box is accepted under any IV size a track
 * can have (0, 8 or 16). */
static rb_rc_t rb_senc(const lm_box_t *bx, int iv)
{
    if (iv >= 0) return rb_senc_iv(bx, (uint8_t)iv);
    static const uint8_t sizes[] = { 0, 8, 16 };
    for (size_t i = 0; i < sizeof(sizes); i++)
        if (rb_senc_iv(bx, sizes[i]) == RB_OK) return RB_OK;
    return RB_MALFORMED;
}

static rb_rc_t rb_saiz(const lm_box_t *bx)
{
    uint8_t ver = 0;
    uint32_t fl = 0;
    if (!lm_fullbox(bx->body, bx->len, &ver, &fl) || ver != 0 || (fl & ~0x1u))
        return RB_MALFORMED;
    size_t fixed = 4 + ((fl & 0x1) ? 8u : 0u) + 1 + 4;
    if (bx->len < fixed) return RB_MALFORMED;
    uint8_t def = bx->body[fixed - 5];
    uint64_t count = lm_rd32(bx->body + fixed - 4);
    return rb_exact(bx->len - fixed, count, def == 0 ? 1u : 0u) ? RB_OK : RB_MALFORMED;
}

static rb_rc_t rb_saio(const lm_box_t *bx)
{
    uint8_t ver = 0;
    uint32_t fl = 0;
    if (!lm_fullbox(bx->body, bx->len, &ver, &fl) || ver > 1 || (fl & ~0x1u))
        return RB_MALFORMED;
    size_t fixed = 4 + ((fl & 0x1) ? 8u : 0u) + 4;
    if (bx->len < fixed) return RB_MALFORMED;
    uint64_t count = lm_rd32(bx->body + fixed - 4);
    return rb_exact(bx->len - fixed, count, ver == 0 ? 4u : 8u) ? RB_OK : RB_MALFORMED;
}

static rb_rc_t rb_sbgp(const lm_box_t *bx)
{
    uint8_t ver = 0;
    uint32_t fl = 0;
    if (!lm_fullbox(bx->body, bx->len, &ver, &fl) || ver > 1) return RB_MALFORMED;
    size_t fixed = 4 + 4 + (ver == 1 ? 4u : 0u) + 4;
    if (bx->len < fixed) return RB_MALFORMED;
    uint64_t count = lm_rd32(bx->body + fixed - 4);
    return rb_exact(bx->len - fixed, count, 8) ? RB_OK : RB_MALFORMED;
}

/* CENC 6 CencSampleEncryptionInformationGroupEntry: reserved, pattern,
 * isProtected, Per_Sample_IV_Size, KID, and a size-prefixed constant IV
 * when protected with IV size 0. */
static rb_rc_t rb_seig_entry(const uint8_t *e, size_t len)
{
    if (len < 20) return RB_MALFORMED;
    if (e[2] == 1 && e[3] == 0) {
        if (len < 21) return RB_MALFORMED;
        return len == 21u + e[20] ? RB_OK : RB_MALFORMED;
    }
    return len == 20 ? RB_OK : RB_MALFORMED;
}

static rb_rc_t rb_sgpd(const lm_box_t *bx)
{
    uint8_t ver = 0;
    uint32_t fl = 0;
    if (!lm_fullbox(bx->body, bx->len, &ver, &fl)) return RB_MALFORMED;
    if (ver != 1 && ver != 2) return RB_UNSUPPORTED;
    size_t fixed = 4 + 4 + 4 + (ver == 2 ? 4u : 0u) + 4;
    if (bx->len < fixed) return RB_MALFORMED;
    uint32_t grouping = lm_rd32(bx->body + 4);
    bool seig = grouping == LM_FOURCC('s','e','i','g');
    if (ver == 2 && !seig) return RB_UNSUPPORTED;
    uint64_t def_len = lm_rd32(bx->body + 8);
    uint64_t count = lm_rd32(bx->body + fixed - 4);
    size_t pos = fixed;
    if (def_len != 0 && !seig)
        return rb_exact(bx->len - pos, count, def_len) ? RB_OK : RB_MALFORMED;
    if (def_len != 0 && !rb_exact(bx->len - pos, count, def_len)) return RB_MALFORMED;
    /* Each entry takes at least four bytes (its length or a seig entry),
     * so the walk is bounded by the box. */
    for (uint64_t i = 0; i < count; i++) {
        uint64_t elen = def_len;
        if (elen == 0) {
            if (bx->len - pos < 4) return RB_MALFORMED;
            elen = lm_rd32(bx->body + pos);
            pos += 4;
        }
        if (elen > bx->len - pos) return RB_MALFORMED;
        if (seig) {
            rb_rc_t rc = rb_seig_entry(bx->body + pos, (size_t)elen);
            if (rc != RB_OK) return rc;
        }
        pos += (size_t)elen;
    }
    return pos == bx->len ? RB_OK : RB_MALFORMED;
}

static rb_rc_t rb_subs(const lm_box_t *bx)
{
    uint8_t ver = 0;
    uint32_t fl = 0;
    if (!lm_fullbox(bx->body, bx->len, &ver, &fl) || ver > 1 || bx->len < 8)
        return RB_MALFORMED;
    uint64_t count = lm_rd32(bx->body + 4);
    size_t pos = 8;
    size_t sub = (ver == 1 ? 4u : 2u) + 1 + 1 + 4;
    /* Every entry takes at least six bytes. */
    for (uint64_t i = 0; i < count; i++) {
        if (bx->len - pos < 6) return RB_MALFORMED;
        uint64_t subs = lm_rd16(bx->body + pos + 4);
        pos += 6;
        if (subs > (bx->len - pos) / sub) return RB_MALFORMED;
        pos += (size_t)subs * sub;
    }
    return pos == bx->len ? RB_OK : RB_MALFORMED;
}

static rb_rc_t rb_pssh(const lm_box_t *bx)
{
    uint8_t ver = 0;
    uint32_t fl = 0;
    if (!lm_fullbox(bx->body, bx->len, &ver, &fl) || ver > 1) return RB_MALFORMED;
    size_t pos = 4 + 16;
    if (bx->len < pos) return RB_MALFORMED;
    if (ver == 1) {
        if (bx->len - pos < 4) return RB_MALFORMED;
        uint64_t kids = lm_rd32(bx->body + pos);
        pos += 4;
        if (kids > (bx->len - pos) / 16) return RB_MALFORMED;
        pos += (size_t)kids * 16;
    }
    if (bx->len - pos < 4) return RB_MALFORMED;
    uint64_t data = lm_rd32(bx->body + pos);
    pos += 4;
    return data == bx->len - pos ? RB_OK : RB_MALFORMED;
}

static rb_rc_t rb_traf(const lm_box_t *traf, int iv)
{
    size_t pos = 0;
    bool first = true;
    while (pos < traf->len) {
        lm_box_t bx;
        rb_rc_t rc = rb_child(traf->body + pos, traf->len - pos, &bx);
        if (rc != RB_OK) return rc;
        if (first) {
            if (bx.type != LM_FOURCC('t','f','h','d')) return RB_MALFORMED;
            rc = rb_tfhd(&bx);
            first = false;
        } else {
            switch (bx.type) {
            case LM_FOURCC('t','f','d','t'): rc = rb_tfdt(&bx); break;
            case LM_FOURCC('t','r','u','n'): rc = rb_trun(&bx); break;
            case LM_FOURCC('s','e','n','c'): rc = rb_senc(&bx, iv); break;
            case LM_FOURCC('s','a','i','z'): rc = rb_saiz(&bx); break;
            case LM_FOURCC('s','a','i','o'): rc = rb_saio(&bx); break;
            case LM_FOURCC('s','b','g','p'): rc = rb_sbgp(&bx); break;
            case LM_FOURCC('s','g','p','d'): rc = rb_sgpd(&bx); break;
            case LM_FOURCC('s','u','b','s'): rc = rb_subs(&bx); break;
            case LM_FOURCC('u','u','i','d'): rc = bx.len >= 16 ? RB_OK : RB_MALFORMED; break;
            default: return RB_UNSUPPORTED;
            }
        }
        if (rc != RB_OK) return rc;
        pos += bx.size;
    }
    return first ? RB_MALFORMED : RB_OK;
}

static rb_rc_t rb_moof(const lm_box_t *moof, int iv)
{
    size_t pos = 0;
    bool first = true;
    size_t trafs = 0;
    while (pos < moof->len) {
        lm_box_t bx;
        rb_rc_t rc = rb_child(moof->body + pos, moof->len - pos, &bx);
        if (rc != RB_OK) return rc;
        if (first) {
            uint8_t ver = 0;
            uint32_t fl = 0;
            if (bx.type != LM_FOURCC('m','f','h','d') ||
                !lm_fullbox(bx.body, bx.len, &ver, &fl) || ver != 0 || bx.len != 8)
                return RB_MALFORMED;
            first = false;
        } else {
            switch (bx.type) {
            case LM_FOURCC('t','r','a','f'): rc = rb_traf(&bx, iv); trafs++; break;
            case LM_FOURCC('p','s','s','h'): rc = rb_pssh(&bx); break;
            case LM_FOURCC('u','u','i','d'): rc = bx.len >= 16 ? RB_OK : RB_MALFORMED; break;
            default: return RB_UNSUPPORTED;
            }
        }
        if (rc != RB_OK) return rc;
        pos += bx.size;
    }
    return trafs ? RB_OK : RB_MALFORMED;
}

moq_result_t locmaf_raw_chunk_check(const moq_locmaf_track_t *track,
                                    moq_bytes_t bytes, moq_locmaf_reason_t *why)
{
    int iv = !track ? -1
           : track->is_protected ? (int)track->default_per_sample_iv_size : 0;
    size_t pos = 0;
    bool styp = false, prft = false, moof = false, mdat = false;
    rb_rc_t rc = RB_OK;
    /* The top level is already known to be whole boxes with plain sizes. */
    while (pos < bytes.len && rc == RB_OK) {
        lm_box_t bx;
        if (lm_box_at(bytes.data + pos, bytes.len - pos, &bx) != LM_BOX_OK) {
            rc = RB_MALFORMED;
            break;
        }
        pos += bx.size;
        if (mdat) { rc = RB_UNSUPPORTED; break; }      /* nothing after mdat */
        if (bx.type == LM_FOURCC('m','d','a','t')) {
            if (!moof) rc = RB_UNSUPPORTED;
            mdat = true;
            continue;
        }
        if (bx.type == LM_FOURCC('m','o','o','f')) {
            if (moof) { rc = RB_UNSUPPORTED; break; }  /* two chunks */
            moof = true;
            rc = rb_moof(&bx, iv);
            continue;
        }
        if (moof || !locmaf_gen_box_type_ok(bx.type)) { rc = RB_UNSUPPORTED; break; }
        bool *seen = bx.type == LM_FOURCC('s','t','y','p') ? &styp
                   : bx.type == LM_FOURCC('p','r','f','t') ? &prft : NULL;
        if (seen && *seen) { rc = RB_MALFORMED; break; }
        if (seen) *seen = true;
        moq_locmaf_reason_t r = MOQ_LOCMAF_REASON_NONE;
        moq_result_t grc = locmaf_gen_box_check(bx.type,
                                                (moq_bytes_t){ bx.body, bx.len }, &r);
        if (grc == MOQ_ERR_UNSUPPORTED) rc = RB_UNSUPPORTED;
        else if (grc != MOQ_OK) rc = RB_MALFORMED;
    }
    if (rc == RB_OK && !(moof && mdat)) rc = RB_UNSUPPORTED;
    if (rc == RB_MALFORMED) {
        lm_why(why, MOQ_LOCMAF_REASON_RAW_BOXES_MALFORMED);
        return MOQ_ERR_PROTO;
    }
    if (rc == RB_UNSUPPORTED) {
        lm_why(why, MOQ_LOCMAF_REASON_RAW_BOXES_UNSUPPORTED);
        return MOQ_ERR_UNSUPPORTED;
    }
    return MOQ_OK;
}

/* -- Element split ---------------------------------------------------------- */

bool locmaf_tuples_framing(moq_bytes_t block)
{
    size_t pos = 0;
    while (pos < block.len) {
        uint64_t id = 0, v = 0;
        size_t n = locmaf_vi64(block.data + pos, block.len - pos, &id);
        if (!n) return false;
        pos += n;
        n = locmaf_vi64(block.data + pos, block.len - pos, &v);
        if (!n) return false;
        pos += n;
        if (id & 1u) {
            if (v > block.len - pos) return false;
            pos += (size_t)v;
        }
    }
    return true;
}

static moq_result_t split_fail(moq_locmaf_reason_t *why, moq_locmaf_reason_t r)
{
    lm_why(why, r);
    return MOQ_ERR_PROTO;
}

moq_result_t locmaf_split(const moq_locmaf_track_t *track, moq_bytes_t payload,
                          locmaf_split_t *out, moq_locmaf_gen_box_t *boxes,
                          size_t cap, moq_locmaf_reason_t *why)
{
    memset(out, 0, sizeof(*out));
    const uint8_t *p = payload.data;
    size_t len = payload.len, pos = 0;
    if (len == 0) return split_fail(why, MOQ_LOCMAF_REASON_EMPTY_OBJECT);

    uint64_t t = 0;
    size_t n = locmaf_vi64(p, len, &t);
    if (!n) return split_fail(why, MOQ_LOCMAF_REASON_TRUNCATED);
    pos = n;

    if (t == MOQ_LOCMAF_ELEMENT_RAW_BOXES) {
        moq_bytes_t rest = { p + pos, len - pos };
        if (!locmaf_raw_boxes_ok(rest))
            return split_fail(why, MOQ_LOCMAF_REASON_RAW_BOXES_MALFORMED);
        moq_result_t rc = locmaf_raw_chunk_check(track, rest, why);
        if (rc != MOQ_OK) return rc;
        out->kind = MOQ_LOCMAF_OBJECT_RAW_BOXES;
        out->raw_boxes = rest;
        return MOQ_OK;
    }

    bool styp = false, prft = false;
    size_t count = 0;
    /* Every pass consumes at least one element, so the loop ends with the
     * payload. */
    while (t == MOQ_LOCMAF_ELEMENT_GEN_BOX) {
        uint64_t box_size = 0;
        n = locmaf_vi64(p + pos, len - pos, &box_size);
        if (!n) return split_fail(why, MOQ_LOCMAF_REASON_TRUNCATED);
        pos += n;
        if (box_size < 4 || box_size > LM_GEN_BOX_SIZE_MAX)
            return split_fail(why, MOQ_LOCMAF_REASON_GEN_BOX_SIZE);
        if (box_size > len - pos) return split_fail(why, MOQ_LOCMAF_REASON_TRUNCATED);
        uint32_t type = lm_rd32(p + pos);
        moq_bytes_t body = { p + pos + 4, (size_t)box_size - 4 };
        pos += (size_t)box_size;
        if (!locmaf_gen_box_type_ok(type))
            return split_fail(why, MOQ_LOCMAF_REASON_GEN_BOX_TYPE);
        bool *seen = type == LM_FOURCC('s','t','y','p') ? &styp
                   : type == LM_FOURCC('p','r','f','t') ? &prft : NULL;
        if (seen && *seen) return split_fail(why, MOQ_LOCMAF_REASON_GEN_BOX_REPEAT);
        if (seen) *seen = true;
        moq_result_t rc = locmaf_gen_box_check(type, body, why);
        if (rc != MOQ_OK) return rc;
        if (count < cap) {
            boxes[count].type = type;
            boxes[count]._reserved = 0;
            boxes[count].payload = body;
        }
        count++;
        if (pos == len) return split_fail(why, MOQ_LOCMAF_REASON_NO_HEADER);
        n = locmaf_vi64(p + pos, len - pos, &t);
        if (!n) return split_fail(why, MOQ_LOCMAF_REASON_TRUNCATED);
        pos += n;
    }

    if (t == MOQ_LOCMAF_ELEMENT_RAW_BOXES)
        return split_fail(why, MOQ_LOCMAF_REASON_RAW_BOXES_NOT_FIRST);
    if (t != MOQ_LOCMAF_ELEMENT_FULL_HEADER && t != MOQ_LOCMAF_ELEMENT_DELTA_HEADER)
        return split_fail(why, MOQ_LOCMAF_REASON_UNKNOWN_ELEMENT);

    uint64_t plen = 0;
    n = locmaf_vi64(p + pos, len - pos, &plen);
    if (!n) return split_fail(why, MOQ_LOCMAF_REASON_TRUNCATED);
    pos += n;
    if (plen > len - pos) return split_fail(why, MOQ_LOCMAF_REASON_TRUNCATED);
    out->properties = (moq_bytes_t){ p + pos, (size_t)plen };
    pos += (size_t)plen;
    out->mdat_payload = (moq_bytes_t){ len > pos ? p + pos : NULL, len - pos };
    if (!locmaf_tuples_framing(out->properties))
        return split_fail(why, MOQ_LOCMAF_REASON_PROPERTY_FRAMING);
    out->kind = t == MOQ_LOCMAF_ELEMENT_FULL_HEADER ? MOQ_LOCMAF_OBJECT_FULL
                                                    : MOQ_LOCMAF_OBJECT_DELTA;
    out->gen_box_count = count;
    return MOQ_OK;
}

moq_result_t moq_locmaf_parse_object(moq_bytes_t payload,
                                     moq_locmaf_object_view_t *out,
                                     size_t out_size,
                                     moq_locmaf_gen_box_t *gen_boxes,
                                     size_t gen_box_cap,
                                     size_t *gen_box_count,
                                     moq_locmaf_reason_t *reason)
{
    lm_why(reason, MOQ_LOCMAF_REASON_NONE);
    if (!out || !gen_box_count || out_size < MOQ_LOCMAF_OBJECT_VIEW_V0_SIZE ||
        (!payload.data && payload.len) || (!gen_boxes && gen_box_cap))
        return MOQ_ERR_INVAL;
    /* No track facts here: a senc inside rawBoxes is accepted under any IV
     * size a track can have; the decoder judges it against the real track. */
    locmaf_split_t sp;
    moq_locmaf_reason_t why = MOQ_LOCMAF_REASON_NONE;
    moq_result_t rc = locmaf_split(NULL, payload, &sp, gen_boxes, gen_box_cap, &why);
    if (rc != MOQ_OK) {
        *gen_box_count = 0;
        lm_why(reason, why);
        return rc;
    }
    moq_locmaf_object_view_t v;
    memset(&v, 0, sizeof(v));
    v.struct_size = (uint32_t)MOQ_LOCMAF_OBJECT_VIEW_V0_SIZE;
    v.kind = sp.kind;
    v.gen_box_count = sp.gen_box_count;
    v.properties = sp.properties;
    v.mdat_payload = sp.mdat_payload;
    v.raw_boxes = sp.raw_boxes;
    memcpy(out, &v, MOQ_LOCMAF_OBJECT_VIEW_V0_SIZE);
    *gen_box_count = sp.gen_box_count;
    return sp.gen_box_count > gen_box_cap ? MOQ_ERR_BUFFER : MOQ_OK;
}

/* -- Tuple pass 1 ------------------------------------------------------------ */

moq_result_t locmaf_tuples(moq_bytes_t block, bool is_full,
                           const moq_locmaf_track_t *track,
                           locmaf_tuples_t *out, moq_locmaf_reason_t *why)
{
    memset(out, 0, sizeof(*out));
    uint64_t unknown[LM_UNKNOWN_FIELDS];
    size_t unknown_n = 0;
    size_t pos = 0;
    /* The block is framed (locmaf_tuples_framing), so each read succeeds;
     * every tuple consumes at least two bytes. */
    while (pos < block.len) {
        uint64_t id = 0, v = 0;
        pos += locmaf_vi64(block.data + pos, block.len - pos, &id);
        pos += locmaf_vi64(block.data + pos, block.len - pos, &v);
        moq_bytes_t bytes = { NULL, 0 };
        if (id & 1u) {
            bytes = (moq_bytes_t){ v ? block.data + pos : NULL, (size_t)v };
            pos += (size_t)v;
        }
        if (id < LM_IDS && (LM_KNOWN_IDS & LM_BIT(id))) {
            if (out->carried & LM_BIT(id)) {
                lm_why(why, MOQ_LOCMAF_REASON_DUPLICATE_FIELD);
                return MOQ_ERR_PROTO;
            }
            if ((id == MOQ_LOCMAF_FIELD_TFDT_BMDT && !is_full) ||
                (id == MOQ_LOCMAF_FIELD_DELETED_IDS && is_full)) {
                lm_why(why, MOQ_LOCMAF_REASON_FIELD_KIND);
                return MOQ_ERR_PROTO;
            }
            if ((LM_CENC_IDS & LM_BIT(id)) && !track->is_protected) {
                lm_why(why, MOQ_LOCMAF_REASON_CLEAR_TRACK_CENC);
                return MOQ_ERR_PROTO;
            }
            out->carried |= LM_BIT(id);
            out->scalar[id] = (id & 1u) ? 0 : v;
            out->bytes[id] = bytes;
            continue;
        }
        /* An unknown ID: skipped by parity, but never repeated. The table
         * is kept sorted for a binary search. */
        size_t lo = 0, hi = unknown_n;
        while (lo < hi) {
            size_t mid = lo + (hi - lo) / 2;
            if (unknown[mid] < id) lo = mid + 1;
            else hi = mid;
        }
        if (lo < unknown_n && unknown[lo] == id) {
            lm_why(why, MOQ_LOCMAF_REASON_DUPLICATE_FIELD);
            return MOQ_ERR_PROTO;
        }
        if (unknown_n == LM_UNKNOWN_FIELDS) {
            lm_why(why, MOQ_LOCMAF_REASON_LIMIT);
            return MOQ_ERR_LIMIT;
        }
        memmove(unknown + lo + 1, unknown + lo, (unknown_n - lo) * sizeof(unknown[0]));
        unknown[lo] = id;
        unknown_n++;
    }
    out->unknown = unknown_n;
    return MOQ_OK;
}
