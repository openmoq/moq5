#include <moq/loc.h>
#include <moq/kvp.h>
#include <moq/wire.h>
#include <string.h>

/*
 * LOC-02 uses transport-17/18 vi64 integer encoding and a different
 * KVP wire format. Until a vi64 codec lands in moq-core, LOC-02
 * parse/encode returns MOQ_ERR_INVAL.
 */

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
                            moq_bytes_t properties,
                            moq_loc_headers_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    moq_loc_headers_init(out);

    if (profile != MOQ_LOC_PROFILE_01)
        return MOQ_ERR_INVAL;

    if (properties.len == 0)
        return MOQ_OK;
    if (!properties.data)
        return MOQ_ERR_INVAL;

    moq_kvp_decoder_t dec;
    moq_kvp_decoder_init(&dec, properties.data, properties.len);

    moq_kvp_entry_t entry;
    moq_result_t rc;
    while ((rc = moq_kvp_decode_next(&dec, &entry)) == MOQ_OK) {
        if (entry.is_varint) {
            uint64_t val = 0;
            if (moq_quic_varint_decode(entry.value, entry.value_len, &val)
                != entry.value_len)
                return MOQ_ERR_PROTO;

            switch (entry.type) {
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
        } else {
            if (entry.type == LOC01_VIDEO_CONFIG) {
                out->has_video_config = true;
                out->video_config.data = entry.value;
                out->video_config.len = entry.value_len;
            }
        }
    }

    if (rc != MOQ_DONE)
        return rc;
    return MOQ_OK;
}

/* -- Encode ---------------------------------------------------------- */

moq_result_t moq_loc_encode(const moq_alloc_t *alloc,
                             moq_loc_profile_t profile,
                             const moq_loc_headers_t *headers,
                             moq_rcbuf_t **out_properties)
{
    if (!alloc || !headers || !out_properties) return MOQ_ERR_INVAL;
    *out_properties = NULL;

    if (profile != MOQ_LOC_PROFILE_01)
        return MOQ_ERR_INVAL;

    if (headers->has_audio_level && headers->audio_level.level > 127)
        return MOQ_ERR_INVAL;
    if (headers->has_video_frame_marking &&
        headers->video_frame_marking.temporal_id > 7)
        return MOQ_ERR_INVAL;
    if (headers->has_timescale)
        return MOQ_ERR_INVAL;

    bool has_any = headers->has_timestamp ||
                   headers->has_video_frame_marking ||
                   headers->has_audio_level ||
                   headers->has_video_config;
    if (!has_any) return MOQ_OK;

    /* Collect varint entries (type, value) and one optional bytes entry.
     * LOC-01 IDs: 0x02 timestamp, 0x04 vfm, 0x06 audio, 0x0d config */
    uint64_t vi_type[3];
    uint64_t vi_val[3];
    size_t vi_count = 0;

    if (headers->has_timestamp) {
        vi_type[vi_count] = LOC01_CAPTURE_TIMESTAMP;
        vi_val[vi_count]  = headers->timestamp;
        vi_count++;
    }

    if (headers->has_video_frame_marking) {
        vi_type[vi_count] = LOC01_VIDEO_FRAME_MARKING;
        vi_val[vi_count]  = encode_video_frame_marking(
            &headers->video_frame_marking);
        vi_count++;
    }

    if (headers->has_audio_level) {
        vi_type[vi_count] = LOC01_AUDIO_LEVEL;
        vi_val[vi_count]  = encode_audio_level(&headers->audio_level);
        vi_count++;
    }

    bool has_config = headers->has_video_config;

    /* Compute total encoded size. */
    size_t total = 0;
    uint64_t prev = 0;
    for (size_t i = 0; i < vi_count; i++) {
        size_t n = moq_kvp_varint_entry_encoded_len(prev, vi_type[i],
                                                      vi_val[i]);
        if (n == 0) return MOQ_ERR_INVAL;
        total += n;
        prev = vi_type[i];
    }
    if (has_config) {
        moq_kvp_entry_t ce;
        memset(&ce, 0, sizeof(ce));
        ce.type = LOC01_VIDEO_CONFIG;
        ce.is_varint = false;
        ce.value = headers->video_config.data;
        ce.value_len = headers->video_config.len;
        size_t n = moq_kvp_entry_encoded_len(prev, &ce);
        if (n == 0) return MOQ_ERR_INVAL;
        total += n;
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
    for (size_t i = 0; i < vi_count; i++) {
        size_t n = moq_kvp_encode_varint_entry(prev, vi_type[i],
                                                vi_val[i],
                                                buf + pos, total - pos);
        if (n == 0) {
            if (heap) alloc->free(buf, total, alloc->ctx);
            return MOQ_ERR_INVAL;
        }
        pos += n;
        prev = vi_type[i];
    }
    if (has_config) {
        moq_kvp_entry_t ce;
        memset(&ce, 0, sizeof(ce));
        ce.type = LOC01_VIDEO_CONFIG;
        ce.is_varint = false;
        ce.value = headers->video_config.data;
        ce.value_len = headers->video_config.len;
        size_t n = moq_kvp_encode_entry(prev, &ce, buf + pos, total - pos);
        if (n == 0) {
            if (heap) alloc->free(buf, total, alloc->ctx);
            return MOQ_ERR_INVAL;
        }
        pos += n;
    }

    moq_result_t rc = moq_rcbuf_create(alloc, buf, total, out_properties);
    if (heap) alloc->free(buf, total, alloc->ctx);
    return rc;
}
