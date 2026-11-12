#include <moq/loc.h>
#include <moq/vi64.h>
#include "loc_codec_string.h"
#include <moq/wire.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/*
 * Three LOC revisions are registered, each bound to the transport drafts
 * that carry it (see loc_row_for): LOC-01 ids on drafts 16 and 18, LOC-04
 * and LOC-05 ids on draft 22. LOC-02 is reserved and selected by no row. The
 * KVP INTEGER CODEC is the second axis of the same row, so a draft can never
 * be half-applied: its ids and its integer encoding travel together.
 */

/* -- Property IDs, LOC-01 (draft-ietf-moq-loc-01) -------------------- */

#define LOC01_CAPTURE_TIMESTAMP   0x02u
#define LOC01_VIDEO_FRAME_MARKING 0x04u
#define LOC01_AUDIO_LEVEL         0x06u
#define LOC01_VIDEO_CONFIG        0x0du

/* -- Property IDs, LOC-04 (draft-ietf-moq-loc-04 section 6.1) --------- */

#define LOC04_TIMESCALE           0x08u   /* even: vi64 units per second */
#define LOC04_VIDEO_FRAME_MARKING 0x09u   /* odd: 1..4 RFC 9626 bytes */
#define LOC04_AUDIO_LEVEL         0x0cu   /* even: RFC 6464 byte, low 8 bits */
#define LOC04_VIDEO_CONFIG        0x0du   /* odd: codec extradata */
#define LOC04_AUDIO_CONFIG        0x0fu   /* odd: codec configuration */
#define LOC04_TIMESTAMP           0x10u   /* even: vi64, timescale units */

/* -- Property IDs added by LOC-05 (draft-ietf-moq-loc-05 section 6.1) ------ */

#define LOC05_CODEC_STRING        0x11u   /* odd: UTF-8 WebCodecs codec string */

/* LOC04 preserves the complete declared payload, including opaque fourth-byte
 * forms; this codec does not normalize or invent RFC fields. */
#define LOC04_FRAME_MARKING_MIN_LEN 1u
#define LOC04_FRAME_MARKING_MAX_LEN 4u

/* An odd-type KVP value carries a Length; both drafts cap it at 2^16-1
 * (draft-16 section 1.4.2, draft-18 section 1.4.3). */
#define LOC_KVP_MAX_VALUE_LEN 0xFFFFu

/* -- The reviewed integer-codec table --------------------------------
 *
 * One row per transport draft whose property codec has been read and
 * deliberately registered. Every KVP field -- Delta Type, the odd-type
 * Length, and the even-type value -- goes through the SAME row, so a
 * draft cannot be half-applied. Lookup returns NULL for anything not in
 * the table, which is what makes an unknown or future version fail
 * closed instead of silently borrowing draft-16's encoding.
 */
typedef struct loc_int_codec {
    /* Bytes needed for `value`; 0 when the codec cannot represent it. */
    size_t (*len)(uint64_t value);
    /* Minimal-form encode; 0 on unrepresentable value or short buffer. */
    size_t (*encode)(uint64_t value, uint8_t *buf, size_t buf_len);
    /* Decode one integer; 0 on truncation. */
    size_t (*decode)(const uint8_t *buf, size_t buf_len, uint64_t *out);
    uint64_t max_value;
} loc_int_codec_t;

static const loc_int_codec_t k_codec_d16 = {
    moq_quic_varint_len, moq_quic_varint_encode, moq_quic_varint_decode,
    MOQ_QUIC_VARINT_MAX,
};

static const loc_int_codec_t k_codec_d18 = {
    moq_vi64_len, moq_vi64_encode, moq_vi64_decode,
    UINT64_MAX,
};

/* One row per transport draft: the integer codec its KVPs use, the LOC
 * revisions whose property ids it carries, and the compatibility selection
 * among them. Lookup returns NULL for anything not registered, which is what
 * makes an unknown or future version fail closed instead of silently
 * borrowing a neighbour's encoding or ids. */
typedef struct loc_row {
    const loc_int_codec_t *codec;
    moq_loc_profile_t      profile;    /* moq_loc_profile_for_version */
    uint32_t               revisions;  /* LOC_REV(p) for each registered p */
} loc_row_t;

#define LOC_REV(p) (UINT32_C(1) << (unsigned)(p))

static const loc_row_t k_row_d16 = { &k_codec_d16, MOQ_LOC_PROFILE_01,
                                     LOC_REV(MOQ_LOC_PROFILE_01) };
static const loc_row_t k_row_d18 = { &k_codec_d18, MOQ_LOC_PROFILE_01,
                                     LOC_REV(MOQ_LOC_PROFILE_01) };
static const loc_row_t k_row_d22 = { &k_codec_d18, MOQ_LOC_PROFILE_04,
                                     LOC_REV(MOQ_LOC_PROFILE_04) |
                                     LOC_REV(MOQ_LOC_PROFILE_05) };

static const loc_row_t *loc_row_for(moq_version_t v)
{
    switch (v) {
    case MOQ_VERSION_DRAFT_16: return &k_row_d16;  /* section 1.4 (i)    */
    case MOQ_VERSION_DRAFT_18: return &k_row_d18;  /* section 1.4.1 vi64 */
    case MOQ_VERSION_DRAFT_22: return &k_row_d22;  /* section 8.3 vi64   */
    default: return NULL;   /* unknown/unregistered: fail closed */
    }
}

moq_loc_profile_t moq_loc_profile_for_version(moq_version_t transport_version)
{
    const loc_row_t *row = loc_row_for(transport_version);
    return row ? row->profile : (moq_loc_profile_t)0;
}

uint64_t moq_loc_value_max_for_version(moq_version_t transport_version)
{
    const loc_row_t *row = loc_row_for(transport_version);
    return row ? row->codec->max_value : 0;
}

/* The row for a (version, profile) pair, or NULL when the version is not
 * registered or the profile is not a revision registered for it. */
static const loc_row_t *loc_row_select(moq_version_t v, moq_loc_profile_t p)
{
    const loc_row_t *row = loc_row_for(v);
    if (!row || (unsigned)p == 0 || (unsigned)p > 31 ||
        !(row->revisions & LOC_REV(p)))
        return NULL;
    return row;
}

/* LOC-05 changes no LOC-04 property; it only adds the Codec String. */
static bool loc_profile_is_loc04_family(moq_loc_profile_t p)
{
    return p == MOQ_LOC_PROFILE_04 || p == MOQ_LOC_PROFILE_05;
}

/* -- The bounded KVP walker ------------------------------------------
 *
 * Key-Value-Pair { Delta Type (int), [Length (int),] Value (..) }, with
 * the Length present and the Value a byte sequence exactly when the
 * ABSOLUTE type is odd, and the Value a single integer when it is even
 * (draft-16 section 1.4.2, draft-18 section 1.4.3). The two drafts share
 * this structure exactly and differ only in `codec`, which is why one
 * walker serves both.
 *
 * Bounded by construction: every field length is checked against the
 * bytes that remain before those bytes are read, and each step consumes
 * at least one byte, so the walk terminates on any input.
 */
typedef struct loc_kvp_walker {
    const loc_int_codec_t *codec;
    const uint8_t         *buf;
    size_t                 remaining;
    uint64_t               prev_type;
    bool                   first;
} loc_kvp_walker_t;

typedef struct loc_kvp_item {
    uint64_t       type;       /* resolved absolute type */
    bool           is_varint;  /* type is even */
    uint64_t       value;      /* is_varint */
    const uint8_t *bytes;      /* !is_varint; borrowed from the input */
    size_t         bytes_len;
} loc_kvp_item_t;

static void loc_kvp_walker_init(loc_kvp_walker_t *w,
                                const loc_int_codec_t *codec,
                                const uint8_t *buf, size_t len)
{
    w->codec     = codec;
    w->buf       = buf;
    w->remaining = len;
    w->prev_type = 0;
    w->first     = true;
}

/* MOQ_OK with *item filled, MOQ_DONE at the end of the block, or
 * MOQ_ERR_PROTO on a truncated field, an over-long odd value, or a delta
 * that overflows the absolute type. */
static moq_result_t loc_kvp_next(loc_kvp_walker_t *w, loc_kvp_item_t *item)
{
    if (w->remaining == 0) return MOQ_DONE;

    uint64_t delta = 0;
    size_t n = w->codec->decode(w->buf, w->remaining, &delta);
    if (n == 0) return MOQ_ERR_PROTO;
    w->buf += n;
    w->remaining -= n;

    uint64_t abs_type;
    if (w->first) {
        abs_type = delta;
        w->first = false;
    } else {
        if (delta > UINT64_MAX - w->prev_type) return MOQ_ERR_PROTO;
        abs_type = w->prev_type + delta;
    }
    w->prev_type = abs_type;

    item->type      = abs_type;
    item->is_varint = ((abs_type & 1u) == 0);
    item->value     = 0;
    item->bytes     = NULL;
    item->bytes_len = 0;

    if (item->is_varint) {
        n = w->codec->decode(w->buf, w->remaining, &item->value);
        if (n == 0) return MOQ_ERR_PROTO;
        w->buf += n;
        w->remaining -= n;
        return MOQ_OK;
    }

    uint64_t vlen = 0;
    n = w->codec->decode(w->buf, w->remaining, &vlen);
    if (n == 0) return MOQ_ERR_PROTO;
    w->buf += n;
    w->remaining -= n;

    if (vlen > LOC_KVP_MAX_VALUE_LEN) return MOQ_ERR_PROTO;
    if (vlen > w->remaining) return MOQ_ERR_PROTO;

    item->bytes     = w->buf;
    item->bytes_len = (size_t)vlen;
    w->buf += (size_t)vlen;
    w->remaining -= (size_t)vlen;
    return MOQ_OK;
}

/* Encoded size of one entry, or 0 if the codec cannot represent a field. */
static size_t loc_kvp_item_len(const loc_int_codec_t *codec,
                               uint64_t prev_type,
                               const loc_kvp_item_t *item)
{
    if (item->type < prev_type) return 0;
    if (item->bytes_len > LOC_KVP_MAX_VALUE_LEN) return 0;
    if (item->bytes_len > 0 && !item->bytes) return 0;

    size_t total = codec->len(item->type - prev_type);
    if (total == 0) return 0;

    if (item->is_varint) {
        size_t vl = codec->len(item->value);
        if (vl == 0) return 0;
        if (vl > SIZE_MAX - total) return 0;
        return total + vl;
    }

    size_t ll = codec->len((uint64_t)item->bytes_len);
    if (ll == 0) return 0;
    if (ll > SIZE_MAX - total) return 0;
    total += ll;
    if (item->bytes_len > SIZE_MAX - total) return 0;
    return total + item->bytes_len;
}

/* Write one entry; 0 on any refusal, never a partial write the caller
 * could mistake for progress. */
static size_t loc_kvp_item_encode(const loc_int_codec_t *codec,
                                  uint64_t prev_type,
                                  const loc_kvp_item_t *item,
                                  uint8_t *buf, size_t buf_len)
{
    if (!buf) return 0;
    size_t needed = loc_kvp_item_len(codec, prev_type, item);
    if (needed == 0 || buf_len < needed) return 0;

    size_t pos = codec->encode(item->type - prev_type, buf, buf_len);
    if (pos == 0) return 0;

    if (item->is_varint) {
        size_t vn = codec->encode(item->value, buf + pos, buf_len - pos);
        if (vn == 0) return 0;
        return pos + vn;
    }

    size_t ln = codec->encode((uint64_t)item->bytes_len,
                              buf + pos, buf_len - pos);
    if (ln == 0) return 0;
    pos += ln;
    if (item->bytes_len > 0) {
        memcpy(buf + pos, item->bytes, item->bytes_len);
        pos += item->bytes_len;
    }
    return pos;
}


_Static_assert(sizeof(moq_loc_headers_t) == MOQ_LOC_HEADERS_V0_SIZE,
               "legacy LOC complete layout is frozen");
_Static_assert(sizeof(moq_loc_metadata_t) == MOQ_LOC_METADATA_V0_SIZE,
               "metadata v0 includes its complete current layout");
_Static_assert(sizeof(moq_loc_ext_t) == MOQ_LOC_EXT_V0_SIZE,
               "extension v0 includes its complete first-published layout");

void moq_loc_headers_init(moq_loc_headers_t *h)
{
    if (!h) return;
    memset(h, 0, sizeof(*h));
    h->struct_size = (uint32_t)sizeof(*h);
}

void moq_loc_metadata_init_sized(moq_loc_metadata_t *out, size_t size)
{
    if (!out || size < sizeof(uint32_t)) return;
    size_t n = size < sizeof(*out) ? size : sizeof(*out);
    memset(out, 0, n);
    out->struct_size = (uint32_t)n;
}

void moq_loc_ext_init_sized(moq_loc_ext_t *out, size_t size)
{
    if (!out || size < sizeof(uint32_t)) return;
    size_t n = size < sizeof(*out) ? size : sizeof(*out);
    memset(out, 0, n);
    out->struct_size = (uint32_t)n;
}

/* -- Video frame marking decode/encode ------------------------------- */

static moq_result_t decode_video_frame_marking(
    uint64_t val, moq_loc_video_frame_marking_t *out)
{
    if (val > 0xFFFF) return MOQ_ERR_PROTO;

    bool has_lid = val >= 256;
    uint8_t first = has_lid ? (uint8_t)(val >> 8) : (uint8_t)val;

    out->start_of_frame  = (first & 0x80) != 0;
    out->end_of_frame    = (first & 0x40) != 0;
    out->independent     = (first & 0x20) != 0;
    out->discardable     = (first & 0x10) != 0;
    out->base_layer_sync = (first & 0x08) != 0;
    out->temporal_id     = first & 0x07;
    out->has_layer_id    = has_lid;
    out->layer_id        = has_lid ? (uint8_t)(val & 0xFF) : 0;
    return MOQ_OK;
}

static uint64_t encode_video_frame_marking(
    const moq_loc_video_frame_marking_t *m)
{
    uint8_t first = 0;
    if (m->start_of_frame)  first |= 0x80;
    if (m->end_of_frame)    first |= 0x40;
    if (m->independent)     first |= 0x20;
    if (m->discardable)     first |= 0x10;
    if (m->base_layer_sync) first |= 0x08;
    first |= m->temporal_id & 0x07;

    if (m->has_layer_id)
        return ((uint64_t)first << 8) | m->layer_id;
    return first;
}



/* -- Audio level decode/encode --------------------------------------- */

static moq_result_t decode_audio_level(uint64_t val,
                                        moq_loc_audio_level_t *out)
{
    if (val > 0xFF) return MOQ_ERR_PROTO;

    uint8_t byte = (uint8_t)val;
    out->voice_activity = (byte & 0x80) != 0;
    out->level = byte & 0x7F;
    return MOQ_OK;
}

static uint64_t encode_audio_level(const moq_loc_audio_level_t *al)
{
    uint8_t byte = al->level & 0x7F;
    if (al->voice_activity) byte |= 0x80;
    return byte;
}

/* -- Parse ----------------------------------------------------------- */

static moq_result_t parse_loc01_item(const loc_kvp_item_t *item,
                                     moq_loc_headers_t *out)
{
    if (item->is_varint) {
        /* Duplicate known properties: the last value wins. */
        switch (item->type) {
        case LOC01_CAPTURE_TIMESTAMP:
            out->has_timestamp = true;
            out->timestamp = item->value;
            break;
        case LOC01_VIDEO_FRAME_MARKING: {
            moq_result_t drc = decode_video_frame_marking(
                item->value, &out->video_frame_marking);
            if (drc < 0) return drc;
            out->has_video_frame_marking = true;
            break;
        }
        case LOC01_AUDIO_LEVEL: {
            moq_result_t drc = decode_audio_level(
                item->value, &out->audio_level);
            if (drc < 0) return drc;
            out->has_audio_level = true;
            break;
        }
        default:
            /* A well-formed unknown property is skipped, not an error. */
            break;
        }
    } else if (item->type == LOC01_VIDEO_CONFIG) {
        out->has_video_config = true;
        out->video_config.data = item->bytes;
        out->video_config.len = item->bytes_len;
    }
    return MOQ_OK;
}


moq_result_t moq_loc_parse(moq_version_t transport_version,
                            moq_loc_profile_t profile,
                            moq_bytes_t properties,
                            moq_loc_headers_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    moq_loc_headers_init(out);
    if (profile != MOQ_LOC_PROFILE_01 ||
        (transport_version != MOQ_VERSION_DRAFT_16 &&
         transport_version != MOQ_VERSION_DRAFT_18))
        return MOQ_ERR_INVAL;
    const loc_row_t *row = loc_row_select(transport_version, profile);
    if (properties.len > 0 && !properties.data) return MOQ_ERR_INVAL;
    loc_kvp_walker_t w;
    loc_kvp_walker_init(&w, row->codec, properties.data, properties.len);
    loc_kvp_item_t item;
    moq_result_t rc;
    while ((rc = loc_kvp_next(&w, &item)) == MOQ_OK) {
        rc = parse_loc01_item(&item, out);
        if (rc < 0) return rc;
    }
    return rc == MOQ_DONE ? MOQ_OK : rc;
}

/* -- Lossless parse -------------------------------------------------- */

static moq_result_t loc_metadata_item(moq_loc_profile_t profile,
    const loc_kvp_item_t *item, moq_loc_metadata_t *out, moq_loc_ext_t *ext)
{
    bool loc04 = loc_profile_is_loc04_family(profile);
    if (profile == MOQ_LOC_PROFILE_05 && item->type == LOC05_CODEC_STRING) {
        if (ext->present & MOQ_LOC_EXT_HAS_CODEC_STRING)
            return MOQ_ERR_UNSUPPORTED;
        if (!moq_loc_internal_codec_string_ok(item->bytes, item->bytes_len))
            return MOQ_ERR_PROTO;
        ext->codec_string = (moq_bytes_t){item->bytes, item->bytes_len};
        ext->present |= MOQ_LOC_EXT_HAS_CODEC_STRING;
        return MOQ_OK;
    }
    uint32_t flag = 0;
    if (item->type == (loc04 ? LOC04_TIMESTAMP : LOC01_CAPTURE_TIMESTAMP))
        flag = MOQ_LOC_METADATA_HAS_TIMESTAMP;
    else if (loc04 && item->type == LOC04_TIMESCALE)
        flag = MOQ_LOC_METADATA_HAS_TIMESCALE;
    else if (item->type == (loc04 ? LOC04_VIDEO_FRAME_MARKING : LOC01_VIDEO_FRAME_MARKING))
        flag = MOQ_LOC_METADATA_HAS_MARKING;
    else if (item->type == (loc04 ? LOC04_AUDIO_LEVEL : LOC01_AUDIO_LEVEL))
        flag = MOQ_LOC_METADATA_HAS_AUDIO_LEVEL;
    else if (item->type == LOC01_VIDEO_CONFIG)
        flag = MOQ_LOC_METADATA_HAS_VIDEO_CONFIG;
    else if (loc04 && item->type == LOC04_AUDIO_CONFIG)
        flag = MOQ_LOC_METADATA_HAS_AUDIO_CONFIG;
    if (flag == 0) return MOQ_OK;
    if (out->present & flag) return MOQ_ERR_UNSUPPORTED;

    switch (flag) {
    case MOQ_LOC_METADATA_HAS_TIMESTAMP:
        out->timestamp = item->value;
        break;
    case MOQ_LOC_METADATA_HAS_TIMESCALE:
        out->timescale = item->value;
        break;
    case MOQ_LOC_METADATA_HAS_MARKING:
        if (loc04) {
            if (item->bytes_len < LOC04_FRAME_MARKING_MIN_LEN ||
                item->bytes_len > LOC04_FRAME_MARKING_MAX_LEN)
                return MOQ_ERR_PROTO;
            out->marking_len = (uint8_t)item->bytes_len;
            memcpy(out->marking, item->bytes, item->bytes_len);
        } else {
            if (item->value > UINT32_MAX) return MOQ_ERR_PROTO;
            size_t n = 1;
            while (n < 4 && (item->value >> (n * 8)) != 0) ++n;
            out->marking_len = (uint8_t)n;
            for (size_t i = 0; i < n; ++i)
                out->marking[i] = (uint8_t)(item->value >> ((n - i - 1) * 8));
        }
        break;
    case MOQ_LOC_METADATA_HAS_AUDIO_LEVEL:
        if (item->value > UINT8_MAX) return MOQ_ERR_PROTO;
        out->audio_level = (uint8_t)item->value;
        break;
    case MOQ_LOC_METADATA_HAS_VIDEO_CONFIG:
        out->video_config = (moq_bytes_t){item->bytes, item->bytes_len};
        break;
    case MOQ_LOC_METADATA_HAS_AUDIO_CONFIG:
        out->audio_config = (moq_bytes_t){item->bytes, item->bytes_len};
        break;
    default:
        return MOQ_ERR_INVAL;
    }
    out->present |= flag;
    return MOQ_OK;
}

/* Whether two caller output ranges share a byte. */
static bool loc_ranges_overlap(const void *a, size_t a_len,
                               const void *b, size_t b_len)
{
    uintptr_t pa = (uintptr_t)a, pb = (uintptr_t)b;
    return pa < pb + b_len && pb < pa + a_len;
}

moq_result_t moq_loc_parse_metadata(moq_version_t transport_version,
    moq_loc_profile_t profile, moq_bytes_t properties,
    moq_loc_metadata_t *out, size_t out_size)
{
    return moq_loc_parse_metadata_ex(transport_version, profile, properties,
                                     out, out_size, NULL, 0);
}

moq_result_t moq_loc_parse_metadata_ex(moq_version_t transport_version,
    moq_loc_profile_t profile, moq_bytes_t properties,
    moq_loc_metadata_t *out, size_t out_size,
    moq_loc_ext_t *ext, size_t ext_size)
{
    const loc_row_t *row = loc_row_select(transport_version, profile);
    if (!row || !out || out_size < MOQ_LOC_METADATA_V0_SIZE ||
        (properties.len > 0 && !properties.data))
        return MOQ_ERR_INVAL;
    size_t out_n = out_size < sizeof(moq_loc_metadata_t) ?
        out_size : sizeof(moq_loc_metadata_t);
    size_t ext_n = 0;
    if (ext) {
        if (ext_size < MOQ_LOC_EXT_V0_SIZE) return MOQ_ERR_INVAL;
        ext_n = ext_size < sizeof(moq_loc_ext_t) ? ext_size : sizeof(moq_loc_ext_t);
        if (loc_ranges_overlap(out, out_n, ext, ext_n)) return MOQ_ERR_INVAL;
    }
    moq_loc_metadata_t tmp = {0};
    moq_loc_ext_t tmp_ext = {0};
    loc_kvp_walker_t outer;
    loc_kvp_walker_init(&outer, row->codec, properties.data, properties.len);
    loc_kvp_item_t item;
    moq_result_t rc;
    while ((rc = loc_kvp_next(&outer, &item)) == MOQ_OK) {
        if (item.type == 0x0b) {
            /* Exactly one inner list; borrow its original offsets and reset its
             * delta accumulator. No flattening, allocation, or recursive walk. */
            loc_kvp_walker_t inner;
            loc_kvp_walker_init(&inner, row->codec, item.bytes, item.bytes_len);
            while ((rc = loc_kvp_next(&inner, &item)) == MOQ_OK) {
                if (item.type == 0x0b) return MOQ_ERR_PROTO;
                rc = loc_metadata_item(profile, &item, &tmp, &tmp_ext);
                if (rc < 0) return rc;
            }
            if (rc != MOQ_DONE) return rc;
        } else {
            rc = loc_metadata_item(profile, &item, &tmp, &tmp_ext);
            if (rc < 0) return rc;
        }
    }
    if (rc != MOQ_DONE) return rc;
    tmp.struct_size = (uint32_t)out_n;
    memcpy(out, &tmp, out_n);
    if (ext) {
        tmp_ext.struct_size = (uint32_t)ext_n;
        memcpy(ext, &tmp_ext, ext_n);
    }
    return MOQ_OK;
}

/* -- Shared sorted-property emission -------------------------------- */

static moq_result_t loc_encode_items(const moq_alloc_t *alloc,
    const loc_int_codec_t *codec, const loc_kvp_item_t *props,
    size_t prop_count, moq_rcbuf_t **out_properties)
{
    size_t total = 0;
    uint64_t prev = 0;
    for (size_t i = 0; i < prop_count; ++i) {
        size_t n = loc_kvp_item_len(codec, prev, &props[i]);
        if (n == 0 || n > SIZE_MAX - total) return MOQ_ERR_INVAL;
        total += n;
        prev = props[i].type;
    }
    if (total == 0) {
        *out_properties = NULL;
        return MOQ_OK;
    }
    uint8_t scratch[256];
    uint8_t *buf = scratch;
    if (total > sizeof(scratch)) {
        buf = (uint8_t *)alloc->alloc(total, alloc->ctx);
        if (!buf) return MOQ_ERR_NOMEM;
    }
    size_t pos = 0;
    prev = 0;
    for (size_t i = 0; i < prop_count; ++i) {
        size_t n = loc_kvp_item_encode(codec, prev, &props[i], buf + pos, total - pos);
        if (n == 0) {
            if (buf != scratch) alloc->free(buf, total, alloc->ctx);
            return MOQ_ERR_INVAL;
        }
        pos += n;
        prev = props[i].type;
    }
    if (pos != total) {
        if (buf != scratch) alloc->free(buf, total, alloc->ctx);
        return MOQ_ERR_INVAL;
    }
    moq_rcbuf_t *encoded = NULL;
    moq_result_t rc = moq_rcbuf_create(alloc, buf, pos, &encoded);
    if (buf != scratch) alloc->free(buf, total, alloc->ctx);
    if (rc == MOQ_OK) *out_properties = encoded;
    return rc;
}

/* -- Legacy encode --------------------------------------------------- */

moq_result_t moq_loc_encode(const moq_alloc_t *alloc,
    moq_version_t transport_version, moq_loc_profile_t profile,
    const moq_loc_headers_t *headers, moq_rcbuf_t **out_properties)
{
    if (!alloc || !headers || !out_properties) return MOQ_ERR_INVAL;
    *out_properties = NULL;
    if (profile != MOQ_LOC_PROFILE_01 ||
        (transport_version != MOQ_VERSION_DRAFT_16 &&
         transport_version != MOQ_VERSION_DRAFT_18))
        return MOQ_ERR_INVAL;
    const loc_int_codec_t *codec = loc_row_select(transport_version, profile)->codec;
    if ((headers->has_audio_level && headers->audio_level.level > 127) ||
        (headers->has_video_frame_marking && headers->video_frame_marking.temporal_id > 7) ||
        headers->has_timescale ||
        (headers->has_timestamp && headers->timestamp > codec->max_value))
        return MOQ_ERR_INVAL;
    loc_kvp_item_t props[4];
    size_t count = 0;
    if (headers->has_timestamp)
        props[count++] = (loc_kvp_item_t){.type = LOC01_CAPTURE_TIMESTAMP,
            .is_varint = true, .value = headers->timestamp};
    if (headers->has_video_frame_marking)
        props[count++] = (loc_kvp_item_t){.type = LOC01_VIDEO_FRAME_MARKING,
            .is_varint = true, .value = encode_video_frame_marking(&headers->video_frame_marking)};
    if (headers->has_audio_level)
        props[count++] = (loc_kvp_item_t){.type = LOC01_AUDIO_LEVEL,
            .is_varint = true, .value = encode_audio_level(&headers->audio_level)};
    if (headers->has_video_config)
        props[count++] = (loc_kvp_item_t){.type = LOC01_VIDEO_CONFIG,
            .bytes = headers->video_config.data, .bytes_len = headers->video_config.len};
    return loc_encode_items(alloc, codec, props, count, out_properties);
}

/* -- Lossless encode ------------------------------------------------- */

moq_result_t moq_loc_encode_metadata(const moq_alloc_t *alloc,
    moq_version_t transport_version, moq_loc_profile_t profile,
    const moq_loc_metadata_t *in, moq_rcbuf_t **out_properties)
{
    return moq_loc_encode_metadata_ex(alloc, transport_version, profile, in,
                                      NULL, out_properties);
}

moq_result_t moq_loc_encode_metadata_ex(const moq_alloc_t *alloc,
    moq_version_t transport_version, moq_loc_profile_t profile,
    const moq_loc_metadata_t *in, const moq_loc_ext_t *ext,
    moq_rcbuf_t **out_properties)
{
    const loc_row_t *row = loc_row_select(transport_version, profile);
    if (!row || !alloc || !alloc->alloc || !alloc->free || !in || !out_properties ||
        in->struct_size < MOQ_LOC_METADATA_V0_SIZE)
        return MOQ_ERR_INVAL;
    if ((in->present & ~MOQ_LOC_METADATA_PRESENT_MASK) ||
        in->reserved[0] || in->reserved[1])
        return MOQ_ERR_INVAL;
    if (ext && (ext->struct_size < MOQ_LOC_EXT_V0_SIZE ||
                (ext->present & ~MOQ_LOC_EXT_PRESENT_MASK)))
        return MOQ_ERR_INVAL;
    bool codec = ext && (ext->present & MOQ_LOC_EXT_HAS_CODEC_STRING);
    if (codec && profile != MOQ_LOC_PROFILE_05) return MOQ_ERR_UNSUPPORTED;
    if (codec && (ext->codec_string.len > LOC_KVP_MAX_VALUE_LEN ||
                  !moq_loc_internal_codec_string_ok(ext->codec_string.data,
                                                    ext->codec_string.len)))
        return MOQ_ERR_INVAL;
    bool loc04 = loc_profile_is_loc04_family(profile);
    if (!loc04 && (in->present & (MOQ_LOC_METADATA_HAS_TIMESCALE | MOQ_LOC_METADATA_HAS_AUDIO_CONFIG)))
        return MOQ_ERR_UNSUPPORTED;
    if (((in->present & MOQ_LOC_METADATA_HAS_TIMESTAMP) && in->timestamp > row->codec->max_value) ||
        ((in->present & MOQ_LOC_METADATA_HAS_TIMESCALE) && in->timescale > row->codec->max_value))
        return MOQ_ERR_INVAL;
    if ((in->present & MOQ_LOC_METADATA_HAS_MARKING) &&
        (in->marking_len < 1 || in->marking_len > 4))
        return MOQ_ERR_INVAL;
    if (!loc04 && (in->present & MOQ_LOC_METADATA_HAS_MARKING) &&
        in->marking_len > 1 && in->marking[0] == 0)
        return MOQ_ERR_UNSUPPORTED;

    loc_kvp_item_t props[7];
    size_t count = 0;
    if (!loc04 && (in->present & MOQ_LOC_METADATA_HAS_TIMESTAMP))
        props[count++] = (loc_kvp_item_t){.type = LOC01_CAPTURE_TIMESTAMP,
            .is_varint = true, .value = in->timestamp};
    if (in->present & MOQ_LOC_METADATA_HAS_TIMESCALE)
        props[count++] = (loc_kvp_item_t){.type = LOC04_TIMESCALE,
            .is_varint = true, .value = in->timescale};
    if (in->present & MOQ_LOC_METADATA_HAS_MARKING) {
        if (loc04) {
            props[count++] = (loc_kvp_item_t){.type = LOC04_VIDEO_FRAME_MARKING,
                .bytes = in->marking, .bytes_len = in->marking_len};
        } else {
            uint64_t value = 0;
            for (size_t i = 0; i < in->marking_len; ++i)
                value = (value << 8) | in->marking[i];
            props[count++] = (loc_kvp_item_t){.type = LOC01_VIDEO_FRAME_MARKING,
                .is_varint = true, .value = value};
        }
    }
    if (in->present & MOQ_LOC_METADATA_HAS_AUDIO_LEVEL)
        props[count++] = (loc_kvp_item_t){.type = loc04 ? LOC04_AUDIO_LEVEL : LOC01_AUDIO_LEVEL,
            .is_varint = true, .value = in->audio_level};
    if (in->present & MOQ_LOC_METADATA_HAS_VIDEO_CONFIG)
        props[count++] = (loc_kvp_item_t){.type = LOC01_VIDEO_CONFIG,
            .bytes = in->video_config.data, .bytes_len = in->video_config.len};
    if (in->present & MOQ_LOC_METADATA_HAS_AUDIO_CONFIG)
        props[count++] = (loc_kvp_item_t){.type = LOC04_AUDIO_CONFIG,
            .bytes = in->audio_config.data, .bytes_len = in->audio_config.len};
    if (loc04 && (in->present & MOQ_LOC_METADATA_HAS_TIMESTAMP))
        props[count++] = (loc_kvp_item_t){.type = LOC04_TIMESTAMP,
            .is_varint = true, .value = in->timestamp};
    if (codec)
        props[count++] = (loc_kvp_item_t){.type = LOC05_CODEC_STRING,
            .bytes = ext->codec_string.data, .bytes_len = ext->codec_string.len};
    return loc_encode_items(alloc, row->codec, props, count, out_properties);
}
