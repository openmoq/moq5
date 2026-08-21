#include <moq/loc.h>
#include <moq/vi64.h>
#include <moq/wire.h>
#include <string.h>

/*
 * LOC-02 reassigns the property IDs (Timestamp moves to 0x06, which LOC-01
 * uses for Audio Level) and adds a timescale. That mapping is not
 * implemented: LOC-02 parse/encode returns MOQ_ERR_INVAL.
 */

/* -- KVP integer codec (per negotiated draft) ------------------------- *
 * The KVP layout is the same in both drafts; only the variable-length integer
 * differs, so one set of walkers is parameterized by the codec. */

typedef struct loc_vint {
    size_t (*len)(uint64_t value);
    size_t (*encode)(uint64_t value, uint8_t *buf, size_t buf_len);
    size_t (*decode)(const uint8_t *buf, size_t buf_len, uint64_t *out_value);
} loc_vint_t;

static const loc_vint_t loc_vint_quic = {
    moq_quic_varint_len, moq_quic_varint_encode, moq_quic_varint_decode,
};

static const loc_vint_t loc_vint_vi64 = {
    moq_vi64_len, moq_vi64_encode, moq_vi64_decode,
};

/* The integer codec the negotiated draft uses for property KVPs
 * (draft-ietf-moq-transport-18 §1.4.1), or NULL for an unknown version. */
static const loc_vint_t *loc_vint_for(moq_version_t draft)
{
    switch (draft) {
    case MOQ_VERSION_DRAFT_16: return &loc_vint_quic;
    case MOQ_VERSION_DRAFT_18: return &loc_vint_vi64;
    }
    return NULL;
}

/* -- Property IDs (LOC-01, draft-ietf-moq-loc-01) -------------------- */

#define LOC01_CAPTURE_TIMESTAMP   0x02u
#define LOC01_VIDEO_FRAME_MARKING 0x04u
#define LOC01_AUDIO_LEVEL         0x06u
#define LOC01_VIDEO_CONFIG        0x0du

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

moq_result_t moq_loc_parse(moq_loc_profile_t profile,
                            moq_version_t draft,
                            moq_bytes_t properties,
                            moq_loc_headers_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    moq_loc_headers_init(out);

    if (profile != MOQ_LOC_PROFILE_01)
        return MOQ_ERR_INVAL;

    const loc_vint_t *vi = loc_vint_for(draft);
    if (!vi)
        return MOQ_ERR_INVAL;

    if (properties.len == 0)
        return MOQ_OK;
    if (!properties.data)
        return MOQ_ERR_INVAL;

    const uint8_t *p = properties.data;
    size_t rem = properties.len;
    uint64_t prev = 0;

    while (rem > 0) {
        uint64_t delta = 0;
        size_t n = vi->decode(p, rem, &delta);
        if (n == 0) return MOQ_ERR_PROTO;
        p += n; rem -= n;

        if (delta > UINT64_MAX - prev) return MOQ_ERR_PROTO;
        uint64_t type = prev + delta;
        prev = type;

        if ((type & 1u) == 0) {          /* even type: one integer value */
            uint64_t val = 0;
            n = vi->decode(p, rem, &val);
            if (n == 0) return MOQ_ERR_PROTO;
            p += n; rem -= n;

            switch (type) {
            case LOC01_CAPTURE_TIMESTAMP:
                out->has_timestamp = true;
                out->timestamp = val;
                break;
            case LOC01_VIDEO_FRAME_MARKING: {
                moq_result_t drc = decode_video_frame_marking(
                    val, &out->video_frame_marking);
                if (drc < 0) return drc;
                out->has_video_frame_marking = true;
                break;
            }
            case LOC01_AUDIO_LEVEL: {
                moq_result_t drc = decode_audio_level(
                    val, &out->audio_level);
                if (drc < 0) return drc;
                out->has_audio_level = true;
                break;
            }
            default:
                break;
            }
        } else {                         /* odd type: length-prefixed bytes */
            uint64_t vlen = 0;
            n = vi->decode(p, rem, &vlen);
            if (n == 0) return MOQ_ERR_PROTO;
            p += n; rem -= n;

            if (vlen > 0xFFFFu) return MOQ_ERR_PROTO;
            if (vlen > rem) return MOQ_ERR_PROTO;

            if (type == LOC01_VIDEO_CONFIG) {
                out->has_video_config = true;
                out->video_config.data = p;
                out->video_config.len = (size_t)vlen;
            }
            p += (size_t)vlen; rem -= (size_t)vlen;
        }
    }

    return MOQ_OK;
}

/* -- Encode ---------------------------------------------------------- */

/*
 * One planned LOC-01 property. Planning every field into a single list
 * keeps the encoded byte count and the emitted bytes driven by the same
 * iteration: the list is built once, measured, then written.
 */
typedef struct loc01_prop {
    uint64_t       type;
    bool           is_varint;
    uint64_t       val;       /* is_varint */
    const uint8_t *bytes;     /* !is_varint, borrowed */
    size_t         bytes_len;
} loc01_prop_t;

/* Encoded size of one entry, or 0 if the draft's integer encoding cannot
 * represent the type delta, the value, or the byte-field length (the QUIC
 * varint caps at 2^62-1; a byte field caps at 2^16-1 in both drafts). */
static size_t loc01_prop_encoded_len(const loc_vint_t *vi, uint64_t prev,
                                      const loc01_prop_t *p)
{
    if (p->type < prev) return 0;
    size_t total = vi->len(p->type - prev);
    if (total == 0) return 0;

    if (p->is_varint) {
        size_t vlen = vi->len(p->val);
        return vlen ? total + vlen : 0;
    }

    if (p->bytes_len > 0xFFFFu) return 0;
    if (p->bytes_len > 0 && !p->bytes) return 0;
    size_t llen = vi->len(p->bytes_len);
    return llen ? total + llen + p->bytes_len : 0;
}

static size_t loc01_prop_encode(const loc_vint_t *vi, uint64_t prev,
                                 const loc01_prop_t *p,
                                 uint8_t *buf, size_t buf_len)
{
    size_t need = loc01_prop_encoded_len(vi, prev, p);
    if (need == 0 || buf_len < need) return 0;

    size_t pos = vi->encode(p->type - prev, buf, buf_len);
    if (p->is_varint) {
        pos += vi->encode(p->val, buf + pos, buf_len - pos);
        return pos;
    }

    pos += vi->encode(p->bytes_len, buf + pos, buf_len - pos);
    if (p->bytes_len > 0) {
        memcpy(buf + pos, p->bytes, p->bytes_len);
        pos += p->bytes_len;
    }
    return pos;
}

moq_result_t moq_loc_encode(const moq_alloc_t *alloc,
                             moq_loc_profile_t profile,
                             moq_version_t draft,
                             const moq_loc_headers_t *headers,
                             moq_rcbuf_t **out_properties)
{
    if (!alloc || !headers || !out_properties) return MOQ_ERR_INVAL;
    *out_properties = NULL;

    if (profile != MOQ_LOC_PROFILE_01)
        return MOQ_ERR_INVAL;

    const loc_vint_t *vi = loc_vint_for(draft);
    if (!vi)
        return MOQ_ERR_INVAL;

    if (headers->has_audio_level && headers->audio_level.level > 127)
        return MOQ_ERR_INVAL;
    if (headers->has_video_frame_marking &&
        headers->video_frame_marking.temporal_id > 7)
        return MOQ_ERR_INVAL;
    if (headers->has_timescale)
        return MOQ_ERR_INVAL;

    /* Plan every present field in ascending LOC-01 property-ID order:
     * 0x02 timestamp, 0x04 frame marking, 0x06 audio level,
     * 0x0d video config. */
    loc01_prop_t props[4];
    size_t prop_count = 0;

    if (headers->has_timestamp) {
        props[prop_count].type = LOC01_CAPTURE_TIMESTAMP;
        props[prop_count].is_varint = true;
        props[prop_count].val = headers->timestamp;
        props[prop_count].bytes = NULL;
        props[prop_count].bytes_len = 0;
        prop_count++;
    }

    if (headers->has_video_frame_marking) {
        props[prop_count].type = LOC01_VIDEO_FRAME_MARKING;
        props[prop_count].is_varint = true;
        props[prop_count].val = encode_video_frame_marking(
            &headers->video_frame_marking);
        props[prop_count].bytes = NULL;
        props[prop_count].bytes_len = 0;
        prop_count++;
    }

    if (headers->has_audio_level) {
        props[prop_count].type = LOC01_AUDIO_LEVEL;
        props[prop_count].is_varint = true;
        props[prop_count].val = encode_audio_level(&headers->audio_level);
        props[prop_count].bytes = NULL;
        props[prop_count].bytes_len = 0;
        prop_count++;
    }

    if (headers->has_video_config) {
        props[prop_count].type = LOC01_VIDEO_CONFIG;
        props[prop_count].is_varint = false;
        props[prop_count].val = 0;
        props[prop_count].bytes = headers->video_config.data;
        props[prop_count].bytes_len = headers->video_config.len;
        prop_count++;
    }

    /* No fields present means no properties: the caller gets MOQ_OK with a
     * NULL rcbuf. Every path below therefore encodes at least one entry,
     * so the selected buffer is always written before it is copied. */
    if (prop_count == 0) return MOQ_OK;

    /* Compute total encoded size. */
    size_t total = 0;
    uint64_t prev = 0;
    for (size_t i = 0; i < prop_count; i++) {
        size_t n = loc01_prop_encoded_len(vi, prev, &props[i]);
        if (n == 0) return MOQ_ERR_INVAL;
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
        size_t n = loc01_prop_encode(vi, prev, &props[i],
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
