#include <moq/codec_signaling.h>

#include <stddef.h>
#include <string.h>

#define CODEC_STRING_SCRATCH 64

static const char hex_lower[] = "0123456789abcdef";

/* Defined below, once the bit reader it uses is in scope. */
static moq_result_t asc_leading_fields(const uint8_t *d, size_t len,
                                       uint32_t *out_aot);

/* Append one lowercase hex byte (two digits) at pos; returns new pos. */
static size_t put_hex8(char *out, size_t pos, uint8_t v)
{
    out[pos++] = hex_lower[(v >> 4) & 0x0f];
    out[pos++] = hex_lower[v & 0x0f];
    return pos;
}

/* Append an unsigned decimal value at pos; returns new pos. */
static size_t put_u32(char *out, size_t pos, uint32_t v)
{
    char tmp[10];
    size_t n = 0;
    do {
        tmp[n++] = (char)('0' + (v % 10u));
        v /= 10u;
    } while (v != 0);
    while (n > 0) {
        out[pos++] = tmp[--n];
    }
    return pos;
}

/*
 * Sample entry codes are matched by EXACT BYTE EQUALITY. RFC 6381 section 3.3
 * states that these four-character values are case sensitive and defines them
 * with explicit numeric octets, so "Avc1" is not the registered entry "avc1".
 */
static bool entry_is(moq_bytes_t e, const char *want)
{
    return e.len == 4 && memcmp(e.data, want, 4) == 0;
}

static bool entry_matches_format(moq_codec_config_format_t f, moq_bytes_t e)
{
    switch (f) {
    case MOQ_CODEC_CONFIG_AVCC:
        return entry_is(e, "avc1") || entry_is(e, "avc3");
    case MOQ_CODEC_CONFIG_HVCC:
        return entry_is(e, "hvc1") || entry_is(e, "hev1");
    case MOQ_CODEC_CONFIG_AV1C:
        return entry_is(e, "av01");
    case MOQ_CODEC_CONFIG_AAC_ASC:
        return entry_is(e, "mp4a");
    case MOQ_CODEC_CONFIG_OPUS:
        return entry_is(e, "opus");
    default:
        return false;
    }
}

/* Append the 4-byte sample entry followed by '.'; returns new pos. */
static size_t put_sample_entry(char *out, size_t pos, moq_bytes_t entry)
{
    memcpy(out + pos, entry.data, 4);
    pos += 4;
    out[pos++] = '.';
    return pos;
}

/* Format an avc1/avc3 codec string from an avcC record. */
static moq_result_t format_avcc(const moq_codec_string_cfg_t *cfg,
                                char *scratch, size_t *produced)
{
    const uint8_t *p = cfg->decoder_config.data;
    if (cfg->decoder_config.len < 4 || p[0] != 1) {
        return MOQ_ERR_PROTO;
    }

    size_t pos = put_sample_entry(scratch, 0, cfg->sample_entry);
    pos = put_hex8(scratch, pos, p[1]);
    pos = put_hex8(scratch, pos, p[2]);
    pos = put_hex8(scratch, pos, p[3]);
    *produced = pos;
    return MOQ_OK;
}

/*
 * Format an mp4a codec string from an AudioSpecificConfig.
 *
 * RFC 6381 section 3.3: iso-mpega := mp4a "." oti [ "." aud-oti ], and the
 * third element is defined only for OTI 40 ("One of the OTI values for 'mp4a'
 * is 40 ... For this value, the third element identifies the audio
 * ObjectTypeIndication"). Any other OTI therefore has no third element.
 */
#define MP4_OTI_MPEG4_AUDIO 0x40

static moq_result_t format_aac_asc(const moq_codec_string_cfg_t *cfg,
                                   char *scratch, size_t *produced)
{
    uint32_t object_type;
    moq_result_t rc = asc_leading_fields(cfg->decoder_config.data,
                                         cfg->decoder_config.len, &object_type);
    if (rc != MOQ_OK) {
        return rc;
    }

    size_t pos = put_sample_entry(scratch, 0, cfg->sample_entry);
    pos = put_hex8(scratch, pos, cfg->mp4_object_type_indication);
    if (cfg->mp4_object_type_indication == MP4_OTI_MPEG4_AUDIO) {
        scratch[pos++] = '.';
        pos = put_u32(scratch, pos, object_type);
    }
    *produced = pos;
    return MOQ_OK;
}

/* Append an unsigned decimal value at pos, zero-padded to two digits. */
static size_t put_u32_pad2(char *out, size_t pos, uint32_t v)
{
    if (v < 10) {
        out[pos++] = '0';
    }
    return put_u32(out, pos, v);
}

/* Reverse the bit order of a 32-bit value. */
static uint32_t reverse_bits32(uint32_t v)
{
    uint32_t r = 0;
    for (int i = 0; i < 32; i++) {
        r = (r << 1) | (v & 1u);
        v >>= 1;
    }
    return r;
}

/*
 * Format a hev1/hvc1 codec string from an hvcC record, per ISO/IEC
 * 14496-15 Annex E.3.
 */
static moq_result_t format_hevc(const moq_codec_string_cfg_t *cfg,
                                char *scratch, size_t *produced)
{
    const uint8_t *p = cfg->decoder_config.data;
    if (cfg->decoder_config.len < 13 || p[0] != 1) {
        return MOQ_ERR_PROTO;
    }

    uint8_t profile_space = (uint8_t)((p[1] >> 6) & 0x03);
    uint8_t tier_flag = (uint8_t)((p[1] >> 5) & 0x01);
    uint8_t profile_idc = (uint8_t)(p[1] & 0x1f);
    uint32_t compat = ((uint32_t)p[2] << 24) | ((uint32_t)p[3] << 16) |
                      ((uint32_t)p[4] << 8) | (uint32_t)p[5];
    uint8_t level_idc = p[12];

    size_t pos = put_sample_entry(scratch, 0, cfg->sample_entry);
    if (profile_space != 0) {
        scratch[pos++] = (char)('A' + profile_space - 1);
    }
    pos = put_u32(scratch, pos, profile_idc);

    scratch[pos++] = '.';
    uint32_t compat_rev = reverse_bits32(compat);
    if (compat_rev == 0) {
        scratch[pos++] = '0';
    } else {
        char tmp[8];
        size_t n = 0;
        while (compat_rev != 0) {
            tmp[n++] = hex_lower[compat_rev & 0x0f];
            compat_rev >>= 4;
        }
        while (n > 0) {
            scratch[pos++] = tmp[--n];
        }
    }

    scratch[pos++] = '.';
    scratch[pos++] = tier_flag ? 'H' : 'L';
    pos = put_u32(scratch, pos, level_idc);

    /* Constraint bytes p[6..11], trailing zero bytes omitted. */
    size_t last = 0;
    for (size_t i = 0; i < 6; i++) {
        if (p[6 + i] != 0) {
            last = i + 1;
        }
    }
    for (size_t i = 0; i < last; i++) {
        scratch[pos++] = '.';
        pos = put_hex8(scratch, pos, p[6 + i]);
    }

    *produced = pos;
    return MOQ_OK;
}

/*
 * Format an av01 codec string from an av1C record, per the AV1 Codec ISO
 * Media File Format Binding. Produces the mandatory
 * "av01.<profile>.<level><tier>.<bitdepth>" form.
 */
static moq_result_t format_av1(const moq_codec_string_cfg_t *cfg,
                               char *scratch, size_t *produced)
{
    const uint8_t *p = cfg->decoder_config.data;
    if (cfg->decoder_config.len < 4 || (p[0] & 0x7f) != 1) {
        return MOQ_ERR_PROTO;
    }

    uint8_t seq_profile = (uint8_t)((p[1] >> 5) & 0x07);
    uint8_t seq_level_idx = (uint8_t)(p[1] & 0x1f);
    uint8_t tier = (uint8_t)((p[2] >> 7) & 0x01);
    uint8_t high_bitdepth = (uint8_t)((p[2] >> 6) & 0x01);
    uint8_t twelve_bit = (uint8_t)((p[2] >> 5) & 0x01);

    uint32_t bit_depth;
    if (seq_profile == 2 && high_bitdepth) {
        bit_depth = twelve_bit ? 12u : 10u;
    } else {
        bit_depth = high_bitdepth ? 10u : 8u;
    }

    size_t pos = put_sample_entry(scratch, 0, cfg->sample_entry);
    pos = put_u32(scratch, pos, seq_profile);
    scratch[pos++] = '.';
    pos = put_u32_pad2(scratch, pos, seq_level_idx);
    scratch[pos++] = tier ? 'H' : 'M';
    scratch[pos++] = '.';
    pos = put_u32_pad2(scratch, pos, bit_depth);

    *produced = pos;
    return MOQ_OK;
}

/*
 * Format an Opus codec string. Opus carries no parameters in the string
 * (RFC 6381), so the sample entry alone (e.g. "opus") is the whole string.
 */
static moq_result_t format_opus(const moq_codec_string_cfg_t *cfg,
                                char *scratch, size_t *produced)
{
    memcpy(scratch, cfg->sample_entry.data, 4);
    *produced = 4;
    return MOQ_OK;
}

/* -- init_data (decoder configuration record) builder --------------- */

#define AVC_NAL_SPS 7
#define AVC_NAL_PPS 8
#define AVC_MAX_PS  16

#define HEVC_NAL_VPS 32
#define HEVC_NAL_SPS 33
#define HEVC_NAL_PPS 34

typedef struct {
    const uint8_t *p;
    size_t         len;
} nal_ref_t;

/*
 * Iterate the Annex B NAL units of d. On success yields the next NAL with its
 * start code stripped, advances *pos, and returns true; returns false only
 * when no further start code is present.
 *
 * A start code followed by no payload yields *nal_len == 0 rather than ending
 * the walk: an empty NAL is malformed input the caller must reject, and
 * treating it as end-of-stream would silently discard every NAL after it.
 */
static bool next_nal(const uint8_t *d, size_t len, size_t *pos,
                     const uint8_t **nal, size_t *nal_len)
{
    size_t i = *pos;
    while (i + 3 <= len && !(d[i] == 0 && d[i + 1] == 0 && d[i + 2] == 1)) {
        i++;
    }
    if (i + 3 > len) {
        return false;
    }
    size_t start = i + 3;
    size_t j = start;
    while (j + 3 <= len && !(d[j] == 0 && d[j + 1] == 0 && d[j + 2] == 1)) {
        j++;
    }
    size_t end = (j + 3 <= len) ? j : len;
    while (end > start && d[end - 1] == 0) {
        end--;
    }
    *nal = d + start;
    *nal_len = end - start;
    *pos = (j + 3 <= len) ? j : len;
    return true;
}

/* Remove emulation-prevention bytes (00 00 03 -> 00 00) into dst, up to
 * dst_cap bytes. Returns the number of bytes written. */
static size_t deemulate(const uint8_t *src, size_t src_len,
                        uint8_t *dst, size_t dst_cap)
{
    size_t si = 0, di = 0;
    int zeros = 0;
    while (si < src_len && di < dst_cap) {
        uint8_t b = src[si++];
        if (zeros >= 2 && b == 0x03) {
            zeros = 0;
            continue;
        }
        dst[di++] = b;
        zeros = (b == 0) ? zeros + 1 : 0;
    }
    return di;
}

typedef struct {
    const uint8_t *d;
    size_t         len; /* bytes */
    size_t         bitpos;
    bool           overrun;
} bitr_t;

/* Read n bits MSB-first as an unsigned value; sets overrun past the end. */
static uint32_t br_u(bitr_t *b, int n)
{
    uint32_t v = 0;
    for (int i = 0; i < n; i++) {
        size_t byte = b->bitpos >> 3;
        int bit = 7 - (int)(b->bitpos & 7);
        uint32_t x = 0;
        if (byte < b->len) {
            x = (uint32_t)((b->d[byte] >> bit) & 1);
        } else {
            b->overrun = true;
        }
        v = (v << 1) | x;
        b->bitpos++;
    }
    return v;
}

/* unsigned Exp-Golomb (ue(v)) per ISO/IEC 14496-10 9.1 */
static uint32_t br_ue(bitr_t *b)
{
    int lz = 0;
    while (br_u(b, 1) == 0 && lz < 32 && !b->overrun) {
        lz++;
    }
    if (lz == 0) {
        return 0;
    }
    uint32_t suffix = br_u(b, lz);
    return ((uint32_t)1 << lz) - 1 + suffix;
}

/*
 * Validate AudioSpecificConfig's mandatory common leading fields and return
 * the AudioObjectType (ISO/IEC 14496-3). GetAudioObjectType() is a 5-bit
 * field; the value 31 escapes to 32 plus a 6-bit extension that IMMEDIATELY
 * follows it. samplingFrequencyIndex is 4 bits, and the value 15 introduces
 * an explicit 24-bit samplingFrequency. channelConfiguration is 4 bits.
 *
 * The object-type-specific payload that follows is deliberately NOT validated:
 * this is what the codec string consumes, and its syntax differs per object
 * type.
 */
static moq_result_t asc_leading_fields(const uint8_t *d, size_t len,
                                       uint32_t *out_aot)
{
    bitr_t br = { d, len, 0, false };

    uint32_t aot = br_u(&br, 5);
    if (aot == 31) {
        aot = 32u + br_u(&br, 6);
    }
    uint32_t freq_idx = br_u(&br, 4);
    if (freq_idx == 15) {
        br_u(&br, 24);                 /* explicit samplingFrequency */
    }
    br_u(&br, 4);                      /* channelConfiguration */

    if (br.overrun) {
        return MOQ_ERR_PROTO;
    }
    *out_aot = aot;
    return MOQ_OK;
}

static bool avc_profile_has_ext(uint8_t profile_idc)
{
    return profile_idc == 100 || profile_idc == 110 ||
           profile_idc == 122 || profile_idc == 144;
}

/*
 * Parse chroma_format_idc and bit depths from an SPS NAL (payload after
 * the 1-byte NAL header) for High-family profiles, per
 * ISO/IEC 14496-10 7.3.2.1.1. Returns MOQ_OK or MOQ_ERR_PROTO.
 */
static moq_result_t parse_sps_ext(const uint8_t *sps, size_t sps_len,
                                  uint8_t *chroma_format_idc,
                                  uint8_t *bit_depth_luma_minus8,
                                  uint8_t *bit_depth_chroma_minus8)
{
    if (sps_len < 2) {
        return MOQ_ERR_PROTO;
    }
    uint8_t rbsp[32];
    size_t n = deemulate(sps + 1, sps_len - 1, rbsp, sizeof(rbsp));

    bitr_t br = { rbsp, n, 0, false };
    br_u(&br, 8);
    br_u(&br, 8);
    br_u(&br, 8);
    br_ue(&br);
    uint32_t chroma = br_ue(&br);
    if (chroma == 3) {
        br_u(&br, 1);
    }
    uint32_t bd_luma = br_ue(&br);
    uint32_t bd_chroma = br_ue(&br);

    if (br.overrun || chroma > 3 || bd_luma > 7 || bd_chroma > 7) {
        return MOQ_ERR_PROTO;
    }
    *chroma_format_idc = (uint8_t)chroma;
    *bit_depth_luma_minus8 = (uint8_t)bd_luma;
    *bit_depth_chroma_minus8 = (uint8_t)bd_chroma;
    return MOQ_OK;
}

/* Write v as a 16-bit big-endian value at p. */
static void put_be16(uint8_t *p, size_t v)
{
    p[0] = (uint8_t)((v >> 8) & 0xff);
    p[1] = (uint8_t)(v & 0xff);
}

/* Assemble an avcC record from the Annex B SPS/PPS NALs in cfg->source. */
static moq_result_t build_avcc(const moq_codec_init_data_cfg_t *cfg,
                               uint8_t *buf, size_t cap, size_t *out_len)
{
    if (cfg->nal.length_size < 1 || cfg->nal.length_size > 4) {
        return MOQ_ERR_INVAL;
    }

    nal_ref_t sps[AVC_MAX_PS];
    nal_ref_t pps[AVC_MAX_PS];
    size_t nsps = 0, npps = 0;

    size_t pos = 0;
    const uint8_t *nal;
    size_t nal_len;
    while (next_nal(cfg->source.data, cfg->source.len, &pos, &nal, &nal_len)) {
        if (nal_len == 0) {
            return MOQ_ERR_PROTO;      /* empty NAL between start codes */
        }
        uint8_t type = (uint8_t)(nal[0] & 0x1f);
        if (type == AVC_NAL_SPS) {
            if (nsps >= AVC_MAX_PS) return MOQ_ERR_UNSUPPORTED;
            sps[nsps].p = nal;
            sps[nsps].len = nal_len;
            nsps++;
        } else if (type == AVC_NAL_PPS) {
            if (npps >= AVC_MAX_PS) return MOQ_ERR_UNSUPPORTED;
            pps[npps].p = nal;
            pps[npps].len = nal_len;
            npps++;
        }
    }

    /*
     * Out-of-band carriage ('avc1'): a record with no SPS or no PPS configures
     * no decoder. In-band carriage ('avc3') is not expressible yet -- see the
     * CARRIAGE note in the public header.
     */
    if (nsps == 0 || npps == 0) {
        return MOQ_ERR_PROTO;
    }
    if (sps[0].len < 4) {
        return MOQ_ERR_PROTO;
    }
    /* ISO/IEC 14496-15 gives the parameter set length fields 16 bits. A valid
     * but larger parameter set is a record limit, not malformed input. */
    for (size_t i = 0; i < nsps; i++) {
        if (sps[i].len > 0xffffu) return MOQ_ERR_UNSUPPORTED;
    }
    for (size_t i = 0; i < npps; i++) {
        if (pps[i].len > 0xffffu) return MOQ_ERR_UNSUPPORTED;
    }
    uint8_t profile_idc = sps[0].p[1];
    uint8_t profile_compat = sps[0].p[2];
    uint8_t level_idc = sps[0].p[3];

    bool has_ext = avc_profile_has_ext(profile_idc);
    uint8_t chroma = 0, bd_luma = 0, bd_chroma = 0;
    if (has_ext) {
        moq_result_t rc = parse_sps_ext(sps[0].p, sps[0].len,
                                        &chroma, &bd_luma, &bd_chroma);
        if (rc != MOQ_OK) return rc;
    }

    size_t size = 6;
    for (size_t i = 0; i < nsps; i++) size += 2 + sps[i].len;
    size += 1;
    for (size_t i = 0; i < npps; i++) size += 2 + pps[i].len;
    if (has_ext) size += 4;

    *out_len = size;
    if (!buf || cap < size) {
        return MOQ_ERR_BUFFER;
    }

    size_t o = 0;
    buf[o++] = 0x01;
    buf[o++] = profile_idc;
    buf[o++] = profile_compat;
    buf[o++] = level_idc;
    buf[o++] = (uint8_t)(0xfc | (cfg->nal.length_size - 1));
    buf[o++] = (uint8_t)(0xe0 | (nsps & 0x1f));
    for (size_t i = 0; i < nsps; i++) {
        put_be16(buf + o, sps[i].len);
        o += 2;
        memcpy(buf + o, sps[i].p, sps[i].len);
        o += sps[i].len;
    }
    buf[o++] = (uint8_t)(npps & 0xff);
    for (size_t i = 0; i < npps; i++) {
        put_be16(buf + o, pps[i].len);
        o += 2;
        memcpy(buf + o, pps[i].p, pps[i].len);
        o += pps[i].len;
    }
    if (has_ext) {
        buf[o++] = (uint8_t)(0xfc | (chroma & 0x03));
        buf[o++] = (uint8_t)(0xf8 | (bd_luma & 0x07));
        buf[o++] = (uint8_t)(0xf8 | (bd_chroma & 0x07));
        buf[o++] = 0x00;
    }

    return MOQ_OK;
}

/*
 * Parse the fields an hvcC header needs from an HEVC SPS NAL (payload after
 * the 2-byte NAL header), per ISO/IEC 23008-2 7.3.2.2.1. The 12-byte general
 * profile_tier_level maps directly onto hvcC bytes 1..12.
 *
 * When sps_max_sub_layers_minus1 > 0 the SPS carries sub-layer PTL syntax
 * between the general PTL and sps_seq_parameter_set_id. hvcC needs none of it,
 * but it is variable-length and has to be stepped over to reach
 * chroma_format_idc and the bit depths.
 */
static moq_result_t parse_hevc_sps(const uint8_t *sps, size_t sps_len,
                                   uint8_t ptl[12],
                                   uint8_t *num_temporal_layers,
                                   uint8_t *temporal_id_nested,
                                   uint8_t *chroma_format_idc,
                                   uint8_t *bit_depth_luma_minus8,
                                   uint8_t *bit_depth_chroma_minus8)
{
    if (sps_len < 3) {
        return MOQ_ERR_PROTO;
    }
    /* Large enough for the general PTL plus a full set of sub-layer PTLs
     * (7 x 96 bits) and the fields we read after them. */
    uint8_t rbsp[256];
    size_t n = deemulate(sps + 2, sps_len - 2, rbsp, sizeof(rbsp));
    if (n < 13) {
        return MOQ_ERR_PROTO;
    }

    bitr_t br = { rbsp, n, 0, false };
    br_u(&br, 4);                       /* sps_video_parameter_set_id  */
    uint32_t max_sub = br_u(&br, 3);    /* sps_max_sub_layers_minus1   */
    uint32_t tid_nest = br_u(&br, 1);   /* sps_temporal_id_nesting     */

    memcpy(ptl, rbsp + 1, 12);          /* general profile_tier_level  */
    br.bitpos = 8 + 12 * 8;

    /* profile_tier_level() sub-layer part, 23008-2 7.3.3. The general half is
     * the fixed 12 bytes already copied above; what follows is present only
     * when there is more than one temporal sub-layer. max_sub is 3 bits, so
     * it never exceeds 7 and the arrays below are always in range. */
    if (max_sub > 0) {
        bool profile_present[7] = { false };
        bool level_present[7] = { false };
        for (uint32_t i = 0; i < max_sub; i++) {
            profile_present[i] = br_u(&br, 1) != 0;
            level_present[i] = br_u(&br, 1) != 0;
        }
        for (uint32_t i = max_sub; i < 8; i++) {
            br_u(&br, 2);               /* reserved_zero_2bits         */
        }
        for (uint32_t i = 0; i < max_sub; i++) {
            if (profile_present[i]) {
                /* Same layout as the general PTL minus its 8-bit level. */
                br_u(&br, 32);
                br_u(&br, 32);
                br_u(&br, 24);
            }
            if (level_present[i]) {
                br_u(&br, 8);           /* sub_layer_level_idc         */
            }
        }
        if (br.overrun) {
            return MOQ_ERR_PROTO;
        }
    }

    br_ue(&br);                         /* sps_seq_parameter_set_id    */
    uint32_t chroma = br_ue(&br);
    if (chroma == 3) {
        br_u(&br, 1);                   /* separate_colour_plane_flag  */
    }
    br_ue(&br);                         /* pic_width_in_luma_samples   */
    br_ue(&br);                         /* pic_height_in_luma_samples  */
    if (br_u(&br, 1)) {                 /* conformance_window_flag     */
        br_ue(&br); br_ue(&br); br_ue(&br); br_ue(&br);
    }
    uint32_t bd_luma = br_ue(&br);
    uint32_t bd_chroma = br_ue(&br);

    if (br.overrun || chroma > 3 || bd_luma > 7 || bd_chroma > 7) {
        return MOQ_ERR_PROTO;
    }
    *num_temporal_layers = (uint8_t)(max_sub + 1);
    *temporal_id_nested = (uint8_t)tid_nest;
    *chroma_format_idc = (uint8_t)chroma;
    *bit_depth_luma_minus8 = (uint8_t)bd_luma;
    *bit_depth_chroma_minus8 = (uint8_t)bd_chroma;
    return MOQ_OK;
}

/* Append one hvcC NAL-array (array_completeness = 1); returns new offset. */
static size_t put_ps_array(uint8_t *buf, size_t o, uint8_t nal_type,
                           const nal_ref_t *arr, size_t count)
{
    buf[o++] = (uint8_t)(0x80 | (nal_type & 0x3f));
    put_be16(buf + o, count);
    o += 2;
    for (size_t i = 0; i < count; i++) {
        put_be16(buf + o, arr[i].len);
        o += 2;
        memcpy(buf + o, arr[i].p, arr[i].len);
        o += arr[i].len;
    }
    return o;
}

/* Assemble an hvcC record from the Annex B VPS/SPS/PPS NALs in cfg->source. */
static moq_result_t build_hvcc(const moq_codec_init_data_cfg_t *cfg,
                               uint8_t *buf, size_t cap, size_t *out_len)
{
    if (cfg->nal.length_size < 1 || cfg->nal.length_size > 4) {
        return MOQ_ERR_INVAL;
    }

    nal_ref_t vps[AVC_MAX_PS], sps[AVC_MAX_PS], pps[AVC_MAX_PS];
    size_t nvps = 0, nsps = 0, npps = 0;

    size_t pos = 0;
    const uint8_t *nal;
    size_t nal_len;
    while (next_nal(cfg->source.data, cfg->source.len, &pos, &nal, &nal_len)) {
        if (nal_len == 0) {
            return MOQ_ERR_PROTO;      /* empty NAL between start codes */
        }
        uint8_t type = (uint8_t)((nal[0] >> 1) & 0x3f);
        if (type == HEVC_NAL_VPS) {
            if (nvps >= AVC_MAX_PS) return MOQ_ERR_UNSUPPORTED;
            vps[nvps].p = nal; vps[nvps].len = nal_len; nvps++;
        } else if (type == HEVC_NAL_SPS) {
            if (nsps >= AVC_MAX_PS) return MOQ_ERR_UNSUPPORTED;
            sps[nsps].p = nal; sps[nsps].len = nal_len; nsps++;
        } else if (type == HEVC_NAL_PPS) {
            if (npps >= AVC_MAX_PS) return MOQ_ERR_UNSUPPORTED;
            pps[npps].p = nal; pps[npps].len = nal_len; npps++;
        }
    }

    /* Out-of-band carriage ('hvc1'); see the CARRIAGE note in the header. */
    if (nvps == 0 || nsps == 0 || npps == 0) {
        return MOQ_ERR_PROTO;
    }
    for (size_t i = 0; i < nvps; i++) {
        if (vps[i].len > 0xffffu) return MOQ_ERR_UNSUPPORTED;
    }
    for (size_t i = 0; i < nsps; i++) {
        if (sps[i].len > 0xffffu) return MOQ_ERR_UNSUPPORTED;
    }
    for (size_t i = 0; i < npps; i++) {
        if (pps[i].len > 0xffffu) return MOQ_ERR_UNSUPPORTED;
    }

    uint8_t ptl[12], num_tl, tid_nested, chroma, bd_luma, bd_chroma;
    moq_result_t rc = parse_hevc_sps(sps[0].p, sps[0].len, ptl, &num_tl,
                                     &tid_nested, &chroma, &bd_luma, &bd_chroma);
    if (rc != MOQ_OK) {
        return rc;
    }

    size_t size = 23;
    size_t narr = 0;
    if (nvps) { narr++; size += 3; for (size_t i = 0; i < nvps; i++) size += 2 + vps[i].len; }
    if (nsps) { narr++; size += 3; for (size_t i = 0; i < nsps; i++) size += 2 + sps[i].len; }
    if (npps) { narr++; size += 3; for (size_t i = 0; i < npps; i++) size += 2 + pps[i].len; }

    *out_len = size;
    if (!buf || cap < size) {
        return MOQ_ERR_BUFFER;
    }

    size_t o = 0;
    buf[o++] = 0x01;                                   /* configurationVersion */
    memcpy(buf + o, ptl, 12); o += 12;                 /* general PTL          */
    buf[o++] = 0xf0;                                   /* min_spatial_seg (0)  */
    buf[o++] = 0x00;
    buf[o++] = 0xfc;                                   /* parallelismType = 0  */
    buf[o++] = (uint8_t)(0xfc | (chroma & 0x03));
    buf[o++] = (uint8_t)(0xf8 | (bd_luma & 0x07));
    buf[o++] = (uint8_t)(0xf8 | (bd_chroma & 0x07));
    buf[o++] = 0x00; buf[o++] = 0x00;                  /* avgFrameRate         */
    buf[o++] = (uint8_t)(((num_tl & 0x07) << 3) | ((tid_nested & 0x01) << 2) |
                         ((cfg->nal.length_size - 1) & 0x03));
    buf[o++] = (uint8_t)narr;

    if (nvps) o = put_ps_array(buf, o, HEVC_NAL_VPS, vps, nvps);
    if (nsps) o = put_ps_array(buf, o, HEVC_NAL_SPS, sps, nsps);
    if (npps) o = put_ps_array(buf, o, HEVC_NAL_PPS, pps, npps);

    return MOQ_OK;
}

/*
 * Parse an AV1 sequence header OBU payload (AV1 spec 5.5) far enough to fill
 * the two seq_profile/level/tier/color bytes of an av1C. Streams that signal
 * timing_info (and thus a decoder model) are not parsed.
 */
static moq_result_t parse_av1_seqhdr(const uint8_t *p, size_t len,
                                     uint8_t *cfg1, uint8_t *cfg2)
{
    bitr_t br = { p, len, 0, false };
    uint32_t seq_profile = br_u(&br, 3);
    br_u(&br, 1);                          /* still_picture              */
    uint32_t reduced = br_u(&br, 1);       /* reduced_still_picture_hdr  */

    uint32_t level0, tier0 = 0, iddp = 0;
    if (reduced) {
        level0 = br_u(&br, 5);
    } else {
        if (br_u(&br, 1)) {                /* timing_info_present_flag   */
            return MOQ_ERR_UNSUPPORTED;
        }
        iddp = br_u(&br, 1);               /* initial_display_delay      */
        uint32_t opcnt = br_u(&br, 5);     /* operating_points_cnt_m1    */
        br_u(&br, 12);                     /* operating_point_idc[0]     */
        level0 = br_u(&br, 5);
        if (level0 > 7) tier0 = br_u(&br, 1);
        if (iddp && br_u(&br, 1)) br_u(&br, 4);
        for (uint32_t i = 1; i <= opcnt; i++) {
            br_u(&br, 12);
            uint32_t l = br_u(&br, 5);
            if (l > 7) br_u(&br, 1);
            if (iddp && br_u(&br, 1)) br_u(&br, 4);
        }
    }

    uint32_t fwb = br_u(&br, 4);           /* frame_width_bits_minus_1   */
    uint32_t fhb = br_u(&br, 4);           /* frame_height_bits_minus_1  */
    br_u(&br, fwb + 1);                    /* max_frame_width_minus_1    */
    br_u(&br, fhb + 1);                    /* max_frame_height_minus_1   */
    if (!reduced && br_u(&br, 1)) {        /* frame_id_numbers_present   */
        br_u(&br, 4); br_u(&br, 3);
    }
    br_u(&br, 3);                          /* use_128x128 + 2 intra flags */

    uint32_t enable_order_hint = 0;
    if (!reduced) {
        br_u(&br, 4);                      /* interintra/masked/warp/dual */
        enable_order_hint = br_u(&br, 1);
        if (enable_order_hint) br_u(&br, 2);  /* jnt_comp, ref_frame_mvs */
        uint32_t sc_tools = br_u(&br, 1) ? 2u : br_u(&br, 1);
        if (sc_tools > 0 && !br_u(&br, 1)) {
            br_u(&br, 1);                  /* seq_force_integer_mv       */
        }
        if (enable_order_hint) br_u(&br, 3); /* order_hint_bits_minus_1  */
    }
    br_u(&br, 3);                          /* superres, cdef, restoration */

    uint32_t high = br_u(&br, 1);
    uint32_t twelve = 0, bitdepth;
    if (seq_profile == 2 && high) {
        twelve = br_u(&br, 1);
        bitdepth = twelve ? 12u : 10u;
    } else {
        bitdepth = high ? 10u : 8u;
    }
    uint32_t mono = (seq_profile == 1) ? 0u : br_u(&br, 1);

    uint32_t cp = 2, tc = 2, mc = 2;
    if (br_u(&br, 1)) {                    /* color_description_present  */
        cp = br_u(&br, 8);
        tc = br_u(&br, 8);
        mc = br_u(&br, 8);
    }

    /*
     * AV1 1.0.0 section 5.5.2 color_config(): the monochrome branch reads
     * color_range, and the general else branch reads color_range BEFORE
     * deciding subsampling. Only the CP_BT_709/TC_SRGB/MC_IDENTITY branch
     * reads no flag (color_range is 1 by definition there).
     */
    uint32_t ssx, ssy, csp = 0;
    if (mono) {
        br_u(&br, 1);                      /* color_range                */
        ssx = 1; ssy = 1;
    } else if (cp == 1 && tc == 13 && mc == 0) {
        ssx = 0; ssy = 0;
    } else {
        br_u(&br, 1);                      /* color_range                */
        if (seq_profile == 0) {
            ssx = 1; ssy = 1;
        } else if (seq_profile == 1) {
            ssx = 0; ssy = 0;
        } else if (bitdepth == 12) {
            ssx = br_u(&br, 1);
            ssy = ssx ? br_u(&br, 1) : 0u;
        } else {
            ssx = 1; ssy = 0;
        }
        if (ssx && ssy) csp = br_u(&br, 2);
    }

    if (br.overrun) {
        return MOQ_ERR_PROTO;
    }
    *cfg1 = (uint8_t)((seq_profile << 5) | (level0 & 0x1f));
    *cfg2 = (uint8_t)((tier0 << 7) | (high << 6) | (twelve << 5) | (mono << 4) |
                      (ssx << 3) | (ssy << 2) | (csp & 0x03));
    return MOQ_OK;
}

/*
 * Assemble an av1C record from AV1 OBUs in cfg->source. The four header bytes
 * are derived from the sequence header OBU, which is then copied verbatim as
 * the configOBUs (a conformant, un-normalized form).
 */
static moq_result_t build_av1c(const moq_codec_init_data_cfg_t *cfg,
                               uint8_t *buf, size_t cap, size_t *out_len)
{
    const uint8_t *d = cfg->source.data;
    size_t len = cfg->source.len;
    const uint8_t *sh = NULL, *sh_payload = NULL;
    size_t sh_total = 0, sh_payload_len = 0;

    size_t i = 0;
    while (i < len) {
        uint8_t h = d[i];
        uint8_t obu_type = (uint8_t)((h >> 3) & 0x0f);
        uint8_t ext = (uint8_t)((h >> 2) & 0x01);
        uint8_t has_size = (uint8_t)((h >> 1) & 0x01);
        size_t hdr = (size_t)1 + (ext ? 1u : 0u);
        if (i + hdr > len) return MOQ_ERR_PROTO;

        /*
         * The AV1 ISOBMFF binding requires every OBU carried in configOBUs to
         * set obu_has_size_field, so a source OBU without one cannot be copied
         * into a conformant record.
         */
        if (!has_size) {
            return MOQ_ERR_PROTO;
        }

        size_t j = i + hdr;
        size_t payload_size;
        if (has_size) {
            uint64_t val = 0;
            bool terminated = false;
            for (int k = 0; k < 8; k++) {
                if (j >= len) return MOQ_ERR_PROTO;
                uint8_t b = d[j++];
                val |= (uint64_t)(b & 0x7f) << (7 * k);
                if (!(b & 0x80)) { terminated = true; break; }
            }
            /* Every byte carried a continuation bit: the size field never
             * ended, so its assembled value must not be used. */
            if (!terminated) {
                return MOQ_ERR_PROTO;
            }
            payload_size = (size_t)val;
        } else {
            payload_size = len - j;
        }
        if (j + payload_size > len) return MOQ_ERR_PROTO;

        if (obu_type == 1) { /* OBU_SEQUENCE_HEADER */
            sh = d + i;
            sh_total = (j - i) + payload_size;
            sh_payload = d + j;
            sh_payload_len = payload_size;
            break;
        }
        i = j + payload_size;
    }
    if (!sh) {
        return MOQ_ERR_PROTO;
    }

    uint8_t cfg1, cfg2;
    moq_result_t rc = parse_av1_seqhdr(sh_payload, sh_payload_len, &cfg1, &cfg2);
    if (rc != MOQ_OK) {
        return rc;
    }

    size_t size = 4 + sh_total;
    *out_len = size;
    if (!buf || cap < size) {
        return MOQ_ERR_BUFFER;
    }

    buf[0] = 0x81;   /* marker = 1, version = 1 */
    buf[1] = cfg1;
    buf[2] = cfg2;
    buf[3] = 0x00;   /* no initial_presentation_delay */
    memcpy(buf + 4, sh, sh_total);
    return MOQ_OK;
}

/*
 * Structural validation for the records copied through unchanged. The boundary
 * is self-consistency: every count and length a record declares about its own
 * bytes must be satisfiable from those bytes. Decoder semantics are not
 * checked. Each walk uses subtraction against the remaining length so no
 * offset arithmetic can overflow.
 */

/* AVCDecoderConfigurationRecord (ISO/IEC 14496-15). */
static moq_result_t validate_avcc(const uint8_t *d, size_t len)
{
    if (len < 7 || d[0] != 1) {
        return MOQ_ERR_PROTO;
    }
    size_t o = 5;
    size_t nsps = d[o++] & 0x1fu;
    for (size_t i = 0; i < nsps; i++) {
        if (len - o < 2) return MOQ_ERR_PROTO;
        size_t l = ((size_t)d[o] << 8) | d[o + 1];
        o += 2;
        if (l > len - o) return MOQ_ERR_PROTO;
        o += l;
    }
    if (o >= len) return MOQ_ERR_PROTO;
    size_t npps = d[o++];
    for (size_t i = 0; i < npps; i++) {
        if (len - o < 2) return MOQ_ERR_PROTO;
        size_t l = ((size_t)d[o] << 8) | d[o + 1];
        o += 2;
        if (l > len - o) return MOQ_ERR_PROTO;
        o += l;
    }
    /* Trailing profile-extension bytes, when present, are not walked. */
    return MOQ_OK;
}

/* HEVCDecoderConfigurationRecord (ISO/IEC 14496-15). */
static moq_result_t validate_hvcc(const uint8_t *d, size_t len)
{
    if (len < 23 || d[0] != 1) {
        return MOQ_ERR_PROTO;
    }
    size_t o = 22;
    size_t narr = d[o++];
    for (size_t a = 0; a < narr; a++) {
        if (len - o < 3) return MOQ_ERR_PROTO;
        o += 1;                            /* array_completeness / NAL type */
        size_t num = ((size_t)d[o] << 8) | d[o + 1];
        o += 2;
        for (size_t i = 0; i < num; i++) {
            if (len - o < 2) return MOQ_ERR_PROTO;
            size_t l = ((size_t)d[o] << 8) | d[o + 1];
            o += 2;
            if (l > len - o) return MOQ_ERR_PROTO;
            o += l;
        }
    }
    return MOQ_OK;
}

/* AV1CodecConfigurationRecord: four header bytes then the configOBUs. */
static moq_result_t validate_av1c(const uint8_t *d, size_t len)
{
    if (len < 4 || (d[0] & 0x7fu) != 1) {
        return MOQ_ERR_PROTO;
    }
    size_t i = 4;
    while (i < len) {
        uint8_t h = d[i];
        if ((h & 0x02u) == 0) {
            return MOQ_ERR_PROTO;          /* obu_has_size_field required */
        }
        size_t hdr = (size_t)1 + (((h >> 2) & 1u) ? 1u : 0u);
        if (len - i < hdr) return MOQ_ERR_PROTO;
        size_t j = i + hdr;

        uint64_t v = 0;
        int k = 0;
        bool terminated = false;
        for (; k < 8; k++) {
            if (j >= len) return MOQ_ERR_PROTO;
            uint8_t b = d[j++];
            v |= (uint64_t)(b & 0x7fu) << (7 * k);
            if (!(b & 0x80u)) { terminated = true; break; }
        }
        if (!terminated) return MOQ_ERR_PROTO;
        if (v > (uint64_t)(len - j)) return MOQ_ERR_PROTO;
        i = j + (size_t)v;
    }
    return MOQ_OK;
}

/* OpusSpecificBox (dOps). */
static moq_result_t validate_dops(const uint8_t *d, size_t len)
{
    if (len < 11 || d[0] != 0) {
        return MOQ_ERR_PROTO;
    }
    if (d[10] != 0) {
        /* ChannelMappingTable: StreamCount, CoupledCount, mapping[channels]. */
        size_t need = (size_t)11 + 2 + d[1];
        if (len < need) return MOQ_ERR_PROTO;
    }
    return MOQ_OK;
}

/* Validate a minimum length and copy the source through unchanged. */
static moq_result_t passthrough(moq_bytes_t src, size_t min_len,
                                uint8_t *buf, size_t cap, size_t *out_len)
{
    if (src.len < min_len) {
        return MOQ_ERR_PROTO;
    }
    *out_len = src.len;
    if (!buf || cap < src.len) {
        return MOQ_ERR_BUFFER;
    }
    memcpy(buf, src.data, src.len);
    return MOQ_OK;
}

/*
 * Convert an OpusHead (RFC 7845, little-endian, "OpusHead"-prefixed) into an
 * OpusSpecificBox / dOps (ISO base media, big-endian, no magic): the magic is
 * dropped and the multi-byte fields are byte-swapped; the version becomes 0.
 */
static moq_result_t build_dops(moq_bytes_t src, uint8_t *buf, size_t cap,
                               size_t *out_len)
{
    if (src.len < 19 || memcmp(src.data, "OpusHead", 8) != 0) {
        return MOQ_ERR_PROTO;
    }
    const uint8_t *s = src.data;
    uint8_t channels = s[9];
    uint8_t mapping_family = s[18];

    size_t table = 0;
    if (mapping_family != 0) {
        table = (size_t)2 + channels; /* StreamCount, CoupledCount, mapping[] */
        if (src.len < 19 + table) {
            return MOQ_ERR_PROTO;
        }
    }

    size_t size = 11 + table;
    *out_len = size;
    if (!buf || cap < size) {
        return MOQ_ERR_BUFFER;
    }

    buf[0] = 0x00;              /* OpusSpecificBox Version */
    buf[1] = channels;
    buf[2] = s[11]; buf[3] = s[10];             /* PreSkip */
    buf[4] = s[15]; buf[5] = s[14];
    buf[6] = s[13]; buf[7] = s[12];             /* InputSampleRate */
    buf[8] = s[17]; buf[9] = s[16];             /* OutputGain */
    buf[10] = mapping_family;
    if (table) {
        memcpy(buf + 11, s + 19, table);
    }
    return MOQ_OK;
}

void moq_codec_init_data_cfg_init(moq_codec_init_data_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = (uint32_t)sizeof(*cfg);
    cfg->nal.length_size = 4;
}

moq_result_t moq_codec_init_data_build(const moq_codec_init_data_cfg_t *cfg,
                                       uint8_t *buf, size_t cap, size_t *out_len)
{
    if (!cfg || !out_len) {
        return MOQ_ERR_INVAL;
    }
    /*
     * The documented contract: *out_len is 0 on every failure other than
     * MOQ_ERR_BUFFER. Clearing it here means every error return below
     * satisfies that without repeating itself, and the paths that do report a
     * required length set it explicitly.
     */
    *out_len = 0;
    if (cfg->struct_size <
        offsetof(moq_codec_init_data_cfg_t, nal) + sizeof(cfg->nal)) {
        return MOQ_ERR_INVAL;
    }
    if (!cfg->source.data) {
        return MOQ_ERR_INVAL;
    }

    switch (cfg->source_format) {
    case MOQ_CODEC_SOURCE_AVC_ANNEXB:
        return build_avcc(cfg, buf, cap, out_len);
    case MOQ_CODEC_SOURCE_AVC_AVCC: {
        moq_result_t rc = validate_avcc(cfg->source.data, cfg->source.len);
        if (rc != MOQ_OK) return rc;
        return passthrough(cfg->source, 7, buf, cap, out_len);
    }
    case MOQ_CODEC_SOURCE_HEVC_HVCC: {
        moq_result_t rc = validate_hvcc(cfg->source.data, cfg->source.len);
        if (rc != MOQ_OK) return rc;
        return passthrough(cfg->source, 23, buf, cap, out_len);
    }
    case MOQ_CODEC_SOURCE_AV1_AV1C: {
        moq_result_t rc = validate_av1c(cfg->source.data, cfg->source.len);
        if (rc != MOQ_OK) return rc;
        return passthrough(cfg->source, 4, buf, cap, out_len);
    }
    case MOQ_CODEC_SOURCE_AAC_ASC: {
        uint32_t aot;
        moq_result_t rc = asc_leading_fields(cfg->source.data, cfg->source.len,
                                             &aot);
        if (rc != MOQ_OK) return rc;
        return passthrough(cfg->source, 2, buf, cap, out_len);
    }
    case MOQ_CODEC_SOURCE_OPUS_HEAD:
        return build_dops(cfg->source, buf, cap, out_len);
    case MOQ_CODEC_SOURCE_OPUS_DOPS: {
        moq_result_t rc = validate_dops(cfg->source.data, cfg->source.len);
        if (rc != MOQ_OK) return rc;
        return passthrough(cfg->source, 11, buf, cap, out_len);
    }
    case MOQ_CODEC_SOURCE_HEVC_ANNEXB:
        return build_hvcc(cfg, buf, cap, out_len);
    case MOQ_CODEC_SOURCE_AV1_OBU:
        return build_av1c(cfg, buf, cap, out_len);
    default:
        return MOQ_ERR_UNSUPPORTED;
    }
}

void moq_codec_string_cfg_init(moq_codec_string_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = (uint32_t)sizeof(*cfg);
}

moq_result_t moq_codec_string_format(const moq_codec_string_cfg_t *cfg,
                                     uint8_t *buf, size_t cap, size_t *out_len)
{
    if (!cfg || !out_len) {
        return MOQ_ERR_INVAL;
    }
    *out_len = 0;
    if (cfg->struct_size <
        offsetof(moq_codec_string_cfg_t, decoder_config) +
            sizeof(cfg->decoder_config)) {
        return MOQ_ERR_INVAL;
    }
    if (!cfg->sample_entry.data || cfg->sample_entry.len != 4) {
        return MOQ_ERR_INVAL;
    }
    if (!cfg->decoder_config.data) {
        return MOQ_ERR_INVAL;
    }

    switch (cfg->config_format) {
    case MOQ_CODEC_CONFIG_AVCC:
    case MOQ_CODEC_CONFIG_HVCC:
    case MOQ_CODEC_CONFIG_AV1C:
    case MOQ_CODEC_CONFIG_OPUS:
        if (cfg->has_mp4_object_type_indication) return MOQ_ERR_INVAL;
        break;
    case MOQ_CODEC_CONFIG_AAC_ASC:
        if (!cfg->has_mp4_object_type_indication) return MOQ_ERR_INVAL;
        break;
    default:
        return MOQ_ERR_UNSUPPORTED;
    }

    if (!entry_matches_format(cfg->config_format, cfg->sample_entry)) {
        return MOQ_ERR_INVAL;
    }

    char scratch[CODEC_STRING_SCRATCH];
    size_t produced = 0;
    moq_result_t rc;

    switch (cfg->config_format) {
    case MOQ_CODEC_CONFIG_AVCC:
        rc = format_avcc(cfg, scratch, &produced);
        break;
    case MOQ_CODEC_CONFIG_AAC_ASC:
        rc = format_aac_asc(cfg, scratch, &produced);
        break;
    case MOQ_CODEC_CONFIG_HVCC:
        rc = format_hevc(cfg, scratch, &produced);
        break;
    case MOQ_CODEC_CONFIG_AV1C:
        rc = format_av1(cfg, scratch, &produced);
        break;
    case MOQ_CODEC_CONFIG_OPUS:
        rc = format_opus(cfg, scratch, &produced);
        break;
    default:
        return MOQ_ERR_UNSUPPORTED;
    }

    if (rc != MOQ_OK) {
        return rc;
    }

    *out_len = produced;
    if (!buf || cap < produced) {
        return MOQ_ERR_BUFFER;
    }

    memcpy(buf, scratch, produced);
    return MOQ_OK;
}
