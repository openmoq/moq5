#include <moq/loc.h>
#include <moq/vi64.h>
#include <moq/wire.h>
#include <stdint.h>
#include <string.h>

/*
 * LOC-02 uses the draft-18 property identifiers and would additionally
 * reassign Audio Level's ID to Timestamp; that profile is still reserved
 * here and parse/encode returns MOQ_ERR_INVAL for it. The KVP INTEGER
 * CODEC is a separate axis and is selected by transport_version below --
 * a draft-18 session carrying LOC-01 properties is a supported and
 * exercised combination, not a profile bump.
 */

/* -- Property IDs (LOC-01, draft-ietf-moq-loc-01) -------------------- */

#define LOC01_CAPTURE_TIMESTAMP   0x02u
#define LOC01_VIDEO_FRAME_MARKING 0x04u
#define LOC01_AUDIO_LEVEL         0x06u
#define LOC01_VIDEO_CONFIG        0x0du

/* -- Property ids per profile ----------------------------------------
 * LOC-01 (drafts 16/18) and LOC-04 (the ids MoQT draft 21 registers in 16.8 and
 * draft-ietf-moq-loc-04 section 2.3). Frame marking is a varint under LOC-01 but a
 * length-prefixed byte string under LOC-04 (its id is odd). */
typedef struct loc_ids {
    uint64_t timestamp, timescale, frame_marking, audio_level, video_config;
    bool     frame_marking_bytes;
    bool     has_timescale;
} loc_ids_t;

static const loc_ids_t k_ids_01 = {
    LOC01_CAPTURE_TIMESTAMP, 0, LOC01_VIDEO_FRAME_MARKING, LOC01_AUDIO_LEVEL,
    LOC01_VIDEO_CONFIG, false, false };
static const loc_ids_t k_ids_04 = {
    0x10u, 0x08u, 0x09u, 0x0Cu, 0x0Du, true, true };

/* The ids for a (transport version, profile) pair; NULL for a pair that has no
 * reviewed meaning, which fails closed rather than emitting another draft's ids. */
static const loc_ids_t *loc_ids_for(moq_version_t v, moq_loc_profile_t p)
{
    if (p == MOQ_LOC_PROFILE_01)
        return (v == MOQ_VERSION_DRAFT_16 || v == MOQ_VERSION_DRAFT_18) ? &k_ids_01 : NULL;
    if (p == MOQ_LOC_PROFILE_04)
        return v == MOQ_VERSION_DRAFT_21 ? &k_ids_04 : NULL;
    return NULL;
}

moq_loc_profile_t moq_loc_profile_for_transport(moq_version_t v)
{
    switch (v) {
    case MOQ_VERSION_DRAFT_16:
    case MOQ_VERSION_DRAFT_18: return MOQ_LOC_PROFILE_01;
    case MOQ_VERSION_DRAFT_21: return MOQ_LOC_PROFILE_04;
    default: return (moq_loc_profile_t)0;
    }
}

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

static const loc_int_codec_t *loc_codec_for(moq_version_t v)
{
    switch (v) {
    case MOQ_VERSION_DRAFT_16: return &k_codec_d16;  /* section 1.4 (i)    */
    case MOQ_VERSION_DRAFT_18: return &k_codec_d18;  /* section 1.4.1 vi64 */
    case MOQ_VERSION_DRAFT_21: return &k_codec_d18;  /* vi64, unchanged (draft 21 8.1) */
    default: return NULL;   /* unknown/unregistered: fail closed */
    }
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

void moq_loc_headers_init(moq_loc_headers_t *h)
{
    if (!h) return;
    memset(h, 0, sizeof(*h));
    h->struct_size = sizeof(*h);
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

moq_result_t moq_loc_parse(moq_version_t transport_version,
                            moq_loc_profile_t profile,
                            moq_bytes_t properties,
                            moq_loc_headers_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    moq_loc_headers_init(out);

    /* Both selectors are checked before the empty-block shortcut: an
     * unnamed codec is refused whether or not there are bytes to read. */
    const loc_int_codec_t *codec = loc_codec_for(transport_version);
    if (!codec) return MOQ_ERR_INVAL;

    const loc_ids_t *ids = loc_ids_for(transport_version, profile);
    if (!ids)
        return MOQ_ERR_INVAL;

    if (properties.len == 0)
        return MOQ_OK;
    if (!properties.data)
        return MOQ_ERR_INVAL;

    loc_kvp_walker_t w;
    loc_kvp_walker_init(&w, codec, properties.data, properties.len);

    loc_kvp_item_t item;
    moq_result_t rc;
    while ((rc = loc_kvp_next(&w, &item)) == MOQ_OK) {
        if (item.is_varint) {
            /* Duplicate known properties: the last value wins. */
            if (item.type == ids->timestamp) {
                out->has_timestamp = true;
                out->timestamp = item.value;
            } else if (ids->has_timescale && item.type == ids->timescale) {
                out->has_timescale = true;
                out->timescale = item.value;
            } else if (!ids->frame_marking_bytes && item.type == ids->frame_marking) {
                moq_result_t drc = decode_video_frame_marking(
                    item.value, &out->video_frame_marking);
                if (drc < 0) return drc;
                out->has_video_frame_marking = true;
            } else if (item.type == ids->audio_level) {
                moq_result_t drc = decode_audio_level(
                    item.value, &out->audio_level);
                if (drc < 0) return drc;
                out->has_audio_level = true;
            }
            /* A well-formed unknown property is skipped, not an error. */
        } else {
            if (item.type == ids->video_config) {
                out->has_video_config = true;
                out->video_config.data = item.bytes;
                out->video_config.len = item.bytes_len;
            } else if (ids->frame_marking_bytes && item.type == ids->frame_marking) {
                /* RFC 9626 bytes: the first byte, then an optional layer id. */
                if (item.bytes_len < 1 || item.bytes_len > 4) return MOQ_ERR_PROTO;
                uint64_t v = item.bytes[0];
                if (item.bytes_len >= 2) v = (v << 8) | item.bytes[1];
                moq_result_t drc = decode_video_frame_marking(v, &out->video_frame_marking);
                if (drc < 0) return drc;
                out->has_video_frame_marking = true;
            }
        }
    }

    if (rc != MOQ_DONE)
        return rc;
    return MOQ_OK;
}

/* -- Encode ---------------------------------------------------------- */

moq_result_t moq_loc_encode(const moq_alloc_t *alloc,
                             moq_version_t transport_version,
                             moq_loc_profile_t profile,
                             const moq_loc_headers_t *headers,
                             moq_rcbuf_t **out_properties)
{
    if (!alloc || !headers || !out_properties) return MOQ_ERR_INVAL;
    *out_properties = NULL;

    const loc_int_codec_t *codec = loc_codec_for(transport_version);
    if (!codec) return MOQ_ERR_INVAL;

    const loc_ids_t *ids = loc_ids_for(transport_version, profile);
    if (!ids)
        return MOQ_ERR_INVAL;

    if (headers->has_audio_level && headers->audio_level.level > 127)
        return MOQ_ERR_INVAL;
    if (headers->has_video_frame_marking &&
        headers->video_frame_marking.temporal_id > 7)
        return MOQ_ERR_INVAL;
    if (headers->has_timescale && !ids->has_timescale)
        return MOQ_ERR_INVAL;
    /* A timestamp the SELECTED codec cannot represent is refused here
     * rather than truncated. Under draft-16 that is anything above
     * MOQ_QUIC_VARINT_MAX; under draft-18 the whole uint64 range fits. */
    if (headers->has_timestamp && headers->timestamp > codec->max_value)
        return MOQ_ERR_INVAL;

    /* Plan every present field, then order by property id: the KVP delta
     * encoding needs ascending types, and the two profiles number them
     * differently. */
    loc_kvp_item_t props[5];
    uint8_t fm_bytes[2];
    size_t prop_count = 0;
    memset(props, 0, sizeof(props));

    if (headers->has_timestamp) {
        props[prop_count].type = ids->timestamp;
        props[prop_count].is_varint = true;
        props[prop_count].value = headers->timestamp;
        prop_count++;
    }
    if (headers->has_timescale) {
        props[prop_count].type = ids->timescale;
        props[prop_count].is_varint = true;
        props[prop_count].value = headers->timescale;
        prop_count++;
    }
    if (headers->has_video_frame_marking) {
        props[prop_count].type = ids->frame_marking;
        uint64_t v = encode_video_frame_marking(&headers->video_frame_marking);
        if (ids->frame_marking_bytes) {
            size_t n = 0;
            if (headers->video_frame_marking.has_layer_id) fm_bytes[n++] = (uint8_t)(v >> 8);
            fm_bytes[n++] = (uint8_t)v;
            if (n == 2) { /* first byte then layer id: already in that order */ }
            props[prop_count].is_varint = false;
            props[prop_count].bytes = fm_bytes;
            props[prop_count].bytes_len = n;
        } else {
            props[prop_count].is_varint = true;
            props[prop_count].value = v;
        }
        prop_count++;
    }
    if (headers->has_audio_level) {
        props[prop_count].type = ids->audio_level;
        props[prop_count].is_varint = true;
        props[prop_count].value = encode_audio_level(&headers->audio_level);
        prop_count++;
    }
    if (headers->has_video_config) {
        props[prop_count].type = ids->video_config;
        props[prop_count].is_varint = false;
        props[prop_count].bytes = headers->video_config.data;
        props[prop_count].bytes_len = headers->video_config.len;
        prop_count++;
    }
    for (size_t a = 1; a < prop_count; a++) {          /* insertion sort by id */
        loc_kvp_item_t k = props[a];
        size_t b = a;
        while (b > 0 && props[b - 1].type > k.type) { props[b] = props[b - 1]; b--; }
        props[b] = k;
    }

    /* No fields present means no properties: the caller gets MOQ_OK with a
     * NULL rcbuf. Every path below therefore encodes at least one entry,
     * so the selected buffer is always written before it is copied. */
    if (prop_count == 0) return MOQ_OK;

    /* Compute total encoded size under the selected codec. */
    size_t total = 0;
    uint64_t prev = 0;
    for (size_t i = 0; i < prop_count; i++) {
        size_t n = loc_kvp_item_len(codec, prev, &props[i]);
        if (n == 0) return MOQ_ERR_INVAL;
        if (n > SIZE_MAX - total) return MOQ_ERR_INVAL;
        total += n;
        prev = props[i].type;
    }

    /* Encode into a stack buffer, then copy into an rcbuf. */
    uint8_t scratch[256];
    uint8_t *buf = scratch;
    bool heap = false;
    if (total > sizeof(scratch)) {
        buf = (uint8_t *)alloc->alloc(total, alloc->ctx);
        if (!buf) return MOQ_ERR_NOMEM;
        heap = true;
    }

    size_t pos = 0;
    prev = 0;
    for (size_t i = 0; i < prop_count; i++) {
        size_t n = loc_kvp_item_encode(codec, prev, &props[i],
                                       buf + pos, total - pos);
        if (n == 0) {
            if (heap) alloc->free(buf, total, alloc->ctx);
            return MOQ_ERR_INVAL;
        }
        pos += n;
        prev = props[i].type;
    }

    /* The bytes handed to the rcbuf are exactly the bytes the plan
     * measured; a short write would copy unwritten buffer tail. */
    if (pos != total) {
        if (heap) alloc->free(buf, total, alloc->ctx);
        return MOQ_ERR_INVAL;
    }

    moq_result_t rc = moq_rcbuf_create(alloc, buf, pos, out_properties);
    if (heap) alloc->free(buf, total, alloc->ctx);
    return rc;
}
