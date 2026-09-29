#include <moq/cmaf_packager.h>

#include <stddef.h>
#include <string.h>

/* -- FOURCC ---------------------------------------------------------- */

#define FOURCC(a,b,c,d) \
    (((uint32_t)(a)<<24)|((uint32_t)(b)<<16)|((uint32_t)(c)<<8)|(uint32_t)(d))

/* -- Growable output buffer ------------------------------------------ */

/*
 * Every put_* is a no-op once err is set, so a box-writing sequence needs one
 * check at the end instead of one per field. The buffer moves on a grow, so
 * box sizes are patched by offset rather than through a saved pointer.
 */
typedef struct {
    const moq_alloc_t *alloc;
    uint8_t           *p;
    size_t             len;
    size_t             cap;
    bool               err;
} buf_t;

static void buf_free(buf_t *b)
{
    if (b->p) {
        b->alloc->free(b->p, b->cap, b->alloc->ctx);
        b->p = NULL;
    }
    b->len = 0;
    b->cap = 0;
}

/* Grown by alloc+copy+free: moq_alloc_t only guarantees realloc for session
 * allocators, and a packager has no reason to demand one. */
static bool buf_reserve(buf_t *b, size_t extra)
{
    if (b->err) return false;
    if (extra > (size_t)-1 - b->len) {
        b->err = true;
        return false;
    }
    size_t need = b->len + extra;
    if (need <= b->cap) return true;

    size_t cap = b->cap ? b->cap : 1024;
    while (cap < need) {
        if (cap > (size_t)-1 / 2) {
            cap = need;
            break;
        }
        cap *= 2;
    }

    uint8_t *next = b->alloc->alloc(cap, b->alloc->ctx);
    if (!next) {
        b->err = true;
        return false;
    }
    if (b->len) memcpy(next, b->p, b->len);
    if (b->p) b->alloc->free(b->p, b->cap, b->alloc->ctx);
    b->p = next;
    b->cap = cap;
    return true;
}

static uint8_t *buf_extend(buf_t *b, size_t n)
{
    if (!buf_reserve(b, n)) return NULL;
    uint8_t *at = b->p + b->len;
    b->len += n;
    return at;
}

static void put8(buf_t *b, uint8_t v)
{
    uint8_t *p = buf_extend(b, 1);
    if (p) p[0] = v;
}

static void put16(buf_t *b, uint16_t v)
{
    uint8_t *p = buf_extend(b, 2);
    if (p) { p[0] = (uint8_t)(v >> 8); p[1] = (uint8_t)v; }
}

static void put24(buf_t *b, uint32_t v)
{
    uint8_t *p = buf_extend(b, 3);
    if (p) {
        p[0] = (uint8_t)(v >> 16); p[1] = (uint8_t)(v >> 8); p[2] = (uint8_t)v;
    }
}

/* Big-endian store into an already-reserved slot, for the fields patched in
 * place once their value is known. */
static void poke32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

static void put32(buf_t *b, uint32_t v)
{
    uint8_t *p = buf_extend(b, 4);
    if (p) poke32(p, v);
}

static void put64(buf_t *b, uint64_t v)
{
    put32(b, (uint32_t)(v >> 32));
    put32(b, (uint32_t)v);
}

static void put_bytes(buf_t *b, const void *src, size_t n)
{
    if (n == 0) return;
    uint8_t *p = buf_extend(b, n);
    if (p) memcpy(p, src, n);
}

static void put_zero(buf_t *b, size_t n)
{
    uint8_t *p = buf_extend(b, n);
    if (p) memset(p, 0, n);
}

/* Version byte followed by 24 flag bits: the FullBox preamble. */
static void put_fullbox(buf_t *b, uint8_t version, uint32_t flags)
{
    put8(b, version);
    put24(b, flags);
}

/* Length prefix of a NAL, at the avcC/hvcC-declared width. */
static void put_nal_length(buf_t *b, size_t len, uint8_t width)
{
    for (int shift = (width - 1) * 8; shift >= 0; shift -= 8)
        put8(b, (uint8_t)((len >> shift) & 0xff));
}

/* -- Box open/close -------------------------------------------------- */

/* Writes a placeholder size and the type; box_close() patches the size once
 * the children are in, so no box length is computed up front. */
static size_t box_open(buf_t *b, uint32_t type)
{
    size_t at = b->len;
    put32(b, 0);
    put32(b, type);
    return at;
}

static void box_close(buf_t *b, size_t at)
{
    if (b->err) return;
    size_t size = b->len - at;
    /* A 32-bit size covers everything here: the only box that can approach
     * the limit is mdat, and a 4 GiB access unit is not a real input. */
    if (size > 0xFFFFFFFFu) {
        b->err = true;
        return;
    }
    poke32(b->p + at, (uint32_t)size);
}

/* -- MPEG-4 descriptor lengths --------------------------------------- */

/* Descriptor sizes use the 4-byte expandable form (0x80 continuation on the
 * first three bytes). Minimal encoding is legal too, but the 4-byte form is
 * what reference mp4 muxers emit. */
#define DESCR_LEN_BYTES 4

static void put_descr(buf_t *b, uint8_t tag, size_t payload_len)
{
    put8(b, tag);
    put8(b, (uint8_t)(0x80 | ((payload_len >> 21) & 0x7f)));
    put8(b, (uint8_t)(0x80 | ((payload_len >> 14) & 0x7f)));
    put8(b, (uint8_t)(0x80 | ((payload_len >> 7) & 0x7f)));
    put8(b, (uint8_t)(payload_len & 0x7f));
}

#define DESCR_HDR (1 + DESCR_LEN_BYTES)

/* -- Codec config inspection ----------------------------------------- */

/*
 * lengthSizeMinusOne sits in the low two bits of avcC[4] and hvcC[21]. Taken
 * from the caller's record, so the width used to prefix samples is by
 * construction the width the init segment declares.
 */
static moq_result_t nal_length_size_from_record(moq_cmaf_codec_kind_t kind,
                                                moq_bytes_t rec,
                                                uint8_t *out)
{
    size_t offset;
    size_t min_len;

    if (kind == MOQ_CMAF_CODEC_AVC) {
        offset = 4;
        min_len = 7;
    } else {
        offset = 21;
        min_len = 23;
    }
    if (rec.len < min_len) return MOQ_ERR_PROTO;

    *out = (uint8_t)((rec.data[offset] & 0x03) + 1);
    return MOQ_OK;
}

/* Reads n bits MSB-first from *bit, zero-extending past the end of src: a
 * truncated config degrades to a default instead of reading out of bounds. */
static uint32_t read_bits(moq_bytes_t src, size_t *bit, uint32_t n)
{
    uint32_t v = 0;
    for (uint32_t i = 0; i < n; i++) {
        size_t byte = *bit >> 3;
        uint32_t b = 0;
        if (byte < src.len)
            b = (uint32_t)((src.data[byte] >> (7 - (*bit & 7))) & 1);
        v = (v << 1) | b;
        (*bit)++;
    }
    return v;
}

/*
 * AAC frame length from the AudioSpecificConfig's frameLengthFlag: 960 samples
 * when set, 1024 otherwise. The flag sits behind the object type and the
 * sampling frequency / channel configuration fields, so those get walked
 * first. 1024 when the object type carries no GASpecificConfig.
 */
static uint32_t aac_frame_length(moq_bytes_t asc)
{
    if (asc.len < 2) return 1024;

    size_t bit = 0;

    uint32_t object_type = read_bits(asc, &bit, 5);
    if (object_type == 31) object_type = 32 + read_bits(asc, &bit, 6);
    if (read_bits(asc, &bit, 4) == 0x0f)  /* explicit samplingFrequency */
        read_bits(asc, &bit, 24);
    read_bits(asc, &bit, 4);              /* channelConfiguration */

    if (object_type == 5 || object_type == 29) {
        /* SBR / PS: an explicit extension config precedes the GA config. */
        if (read_bits(asc, &bit, 4) == 0x0f)
            read_bits(asc, &bit, 24);
        object_type = read_bits(asc, &bit, 5);
        if (object_type == 31) object_type = 32 + read_bits(asc, &bit, 6);
    }

    switch (object_type) {
    /* Object types whose GASpecificConfig opens with frameLengthFlag. */
    case 1: case 2: case 3: case 4: case 6: case 7:
    case 17: case 19: case 20: case 21: case 22: case 23:
        return read_bits(asc, &bit, 1) ? 960 : 1024;
    default:
        return 1024;
    }
}

/*
 * Number of 48 kHz samples an Opus packet decodes to, read from its TOC byte
 * (RFC 6716 3.1). Frames run 2.5 to 60 ms and a packet holds up to 120 ms;
 * only the packet says which, never the dOps record. 0 for a packet too short
 * or malformed to classify.
 */
static uint32_t opus_packet_samples(moq_bytes_t pkt)
{
    if (pkt.len < 1) return 0;

    const uint8_t toc = pkt.data[0];
    const uint8_t config = (uint8_t)(toc >> 3);

    /* Frame length per configuration family, in 48 kHz samples. */
    static const uint16_t kSilk[4]   = { 480, 960, 1920, 2880 }; /* 10/20/40/60 ms */
    static const uint16_t kHybrid[2] = { 480, 960 };             /* 10/20 ms */
    static const uint16_t kCelt[4]   = { 120, 240, 480, 960 };   /* 2.5/5/10/20 ms */

    uint32_t frame;
    if (config < 12)
        frame = kSilk[config & 3];
    else if (config < 16)
        frame = kHybrid[config & 1];
    else
        frame = kCelt[config & 3];

    uint32_t frames;
    switch (toc & 0x03) {
    case 0:  frames = 1; break;             /* one frame */
    case 1:  frames = 2; break;             /* two frames, equal size */
    case 2:  frames = 2; break;             /* two frames, different sizes */
    default:                                /* arbitrary count in the next byte */
        if (pkt.len < 2) return 0;
        frames = pkt.data[1] & 0x3f;
        if (frames == 0) return 0;
        break;
    }

    const uint32_t samples = frame * frames;
    /* RFC 6716 3.1: a packet holds at most 120 ms of audio. */
    if (samples > 5760) return 0;

    return samples;
}

/* Length of an ADTS header if the buffer opens with a syncword, else 0. */
static size_t adts_header_len(moq_bytes_t s)
{
    if (s.len < 7) return 0;
    if (s.data[0] != 0xff || (s.data[1] & 0xf6) != 0xf0) return 0;
    /* protection_absent (bit 0 of byte 1) clear means a 2-byte CRC follows. */
    return (s.data[1] & 0x01) ? 7 : 9;
}

/* -- Annex B --------------------------------------------------------- */

/*
 * Offset of the next start code at or after `from`, or n when there is none.
 * *sc_len gets 3 or 4: a zero byte in front of 00 00 01 belongs to the start
 * code, not to the NAL, but only if it lies inside the region being scanned.
 */
static size_t next_start_code(const uint8_t *d, size_t n, size_t from,
                              size_t *sc_len)
{
    for (size_t i = from; i + 3 <= n; i++) {
        if (d[i] == 0 && d[i + 1] == 0 && d[i + 2] == 1) {
            if (i > from && d[i - 1] == 0) {
                *sc_len = 4;
                return i - 1;
            }
            *sc_len = 3;
            return i;
        }
    }
    return n;
}

static bool annexb_to_length_prefixed(buf_t *out, moq_bytes_t s, uint8_t width)
{
    size_t sc = 0;
    size_t pos = next_start_code(s.data, s.len, 0, &sc);
    if (pos == s.len) return false; /* not Annex B at all */
    pos += sc;

    size_t count = 0;
    while (pos < s.len) {
        size_t next_sc = 0;
        size_t end = next_start_code(s.data, s.len, pos, &next_sc);
        if (end > pos) {
            put_nal_length(out, end - pos, width);
            put_bytes(out, s.data + pos, end - pos);
            count++;
        }
        pos = (end == s.len) ? s.len : end + next_sc;
    }

    return count > 0;
}

/* Verify a length-prefixed sample is self-consistent before it reaches an
 * mdat, so a malformed input surfaces here and not in a player. */
static bool length_prefixes_valid(moq_bytes_t s, uint8_t width)
{
    size_t pos = 0;
    size_t count = 0;

    while (pos < s.len) {
        if (s.len - pos < width) return false;
        size_t len = 0;
        for (uint8_t i = 0; i < width; i++)
            len = (len << 8) | s.data[pos + i];
        pos += width;
        if (len == 0 || len > s.len - pos) return false;
        pos += len;
        count++;
    }

    return count > 0;
}

/* -- Timestamp rescaling --------------------------------------------- */

/* Every num/den pair reaching rescale() is bounded to this at create, which
 * keeps the remainder term below 2^62. */
#define RESCALE_MAX 0x7fffffffLL

/*
 * v * num / den, exact and without a 128-bit intermediate: split v into
 * quotient and remainder first. Truncation matches C division for negative v
 * because quotient and remainder carry the same sign. False when the result
 * would not fit, rather than overflowing signed arithmetic.
 */
static bool rescale(int64_t v, int64_t num, int64_t den, int64_t *out)
{
    if (den <= 0 || num <= 0 || num == den) {
        *out = v;
        return true;
    }

    const int64_t q = v / den;
    const int64_t r = v % den;
    /* Leaves room for the remainder term, whose magnitude is below num. */
    if (q > (INT64_MAX - num) / num || q < (INT64_MIN + num) / num)
        return false;

    *out = q * num + (r * num) / den;
    return true;
}

/* a - b, false when the difference does not fit. */
static bool sub_checked(int64_t a, int64_t b, int64_t *out)
{
    if (b < 0 ? a > INT64_MAX + b : a < INT64_MIN + b)
        return false;
    *out = a - b;
    return true;
}

/* -- Packager -------------------------------------------------------- */

struct moq_cmaf_packager {
    const moq_alloc_t       *alloc;

    moq_cmaf_codec_kind_t    codec_kind;
    moq_cmaf_sample_format_t sample_format;
    bool                     is_video;

    uint32_t                 timescale;
    uint32_t                 track_id;
    uint32_t                 default_duration;
    uint32_t                 avg_bitrate;
    uint8_t                  nal_length_size;

    int64_t                  src_num;  /* source timebase -> timescale */
    int64_t                  src_den;

    bool                     rebase;
    bool                     have_origin;
    int64_t                  origin;   /* in source timebase units */

    uint32_t                 sequence_number;

    uint8_t                 *codec_config;
    size_t                   codec_config_len;

    buf_t                    init;     /* ftyp+moov, built once */
    buf_t                    frag;     /* moof+mdat, rebuilt per write */
    buf_t                    mdat;     /* converted samples for this write */
};

/* ISO-BMFF sample_flags: sample_depends_on in bits 24-25,
 * sample_is_non_sync_sample in bit 16. */
#define SAMPLE_FLAGS_SYNC     0x02000000u /* depends_on = 2 (independent) */
#define SAMPLE_FLAGS_NON_SYNC 0x01010000u /* depends_on = 1, non_sync = 1 */

static uint32_t sample_entry_type(moq_cmaf_codec_kind_t kind)
{
    switch (kind) {
    case MOQ_CMAF_CODEC_AVC:  return FOURCC('a','v','c','1');
    case MOQ_CMAF_CODEC_HEVC: return FOURCC('h','v','c','1');
    case MOQ_CMAF_CODEC_AV1:  return FOURCC('a','v','0','1');
    case MOQ_CMAF_CODEC_AAC:  return FOURCC('m','p','4','a');
    case MOQ_CMAF_CODEC_OPUS: return FOURCC('O','p','u','s');
    default:                  return 0;
    }
}

static bool codec_is_video(moq_cmaf_codec_kind_t kind)
{
    return kind == MOQ_CMAF_CODEC_AVC || kind == MOQ_CMAF_CODEC_HEVC ||
           kind == MOQ_CMAF_CODEC_AV1;
}

static bool codec_is_nal(moq_cmaf_codec_kind_t kind)
{
    return kind == MOQ_CMAF_CODEC_AVC || kind == MOQ_CMAF_CODEC_HEVC;
}

/* -- Init segment ---------------------------------------------------- */

static void write_ftyp(buf_t *b)
{
    size_t at = box_open(b, FOURCC('f','t','y','p'));
    put32(b, FOURCC('c','m','f','c'));  /* major_brand: CMAF */
    put32(b, 0);                        /* minor_version */
    put32(b, FOURCC('c','m','f','c'));
    put32(b, FOURCC('i','s','o','6'));
    put32(b, FOURCC('i','s','o','m'));
    box_close(b, at);
}

static void write_codec_config_box(buf_t *b, const moq_cmaf_packager_t *p)
{
    moq_bytes_t rec = { p->codec_config, p->codec_config_len };

    switch (p->codec_kind) {
    case MOQ_CMAF_CODEC_AVC: {
        size_t at = box_open(b, FOURCC('a','v','c','C'));
        put_bytes(b, rec.data, rec.len);
        box_close(b, at);
        break;
    }
    case MOQ_CMAF_CODEC_HEVC: {
        size_t at = box_open(b, FOURCC('h','v','c','C'));
        put_bytes(b, rec.data, rec.len);
        box_close(b, at);
        break;
    }
    case MOQ_CMAF_CODEC_AV1: {
        size_t at = box_open(b, FOURCC('a','v','1','C'));
        put_bytes(b, rec.data, rec.len);
        box_close(b, at);
        break;
    }
    case MOQ_CMAF_CODEC_OPUS: {
        size_t at = box_open(b, FOURCC('d','O','p','s'));
        put_bytes(b, rec.data, rec.len);
        box_close(b, at);
        break;
    }
    case MOQ_CMAF_CODEC_AAC: {
        /*
         * esds: ES_Descriptor( DecoderConfigDescriptor( DecoderSpecificInfo )
         * + SLConfigDescriptor ). Sizes are computed bottom-up because a
         * descriptor length precedes its payload.
         */
        const size_t dsi_payload = rec.len;
        const size_t dcd_payload = 13 + DESCR_HDR + dsi_payload;
        const size_t sl_payload  = 1;
        const size_t esd_payload = 3 + DESCR_HDR + dcd_payload
                                     + DESCR_HDR + sl_payload;

        size_t at = box_open(b, FOURCC('e','s','d','s'));
        put_fullbox(b, 0, 0);

        put_descr(b, 0x03, esd_payload);
        put16(b, (uint16_t)p->track_id);  /* ES_ID */
        put8(b, 0);                       /* no dependency, no URL, no OCR */

        put_descr(b, 0x04, dcd_payload);
        put8(b, 0x40);                    /* objectTypeIndication: AAC */
        /* streamType=AudioStream(0x05)<<2 | upStream=0 | reserved=1 */
        put8(b, 0x15);
        put24(b, 0);                      /* bufferSizeDB: not tracked */
        put32(b, p->avg_bitrate);         /* maxBitrate */
        put32(b, p->avg_bitrate);         /* avgBitrate */

        put_descr(b, 0x05, dsi_payload);
        put_bytes(b, rec.data, rec.len);

        put_descr(b, 0x06, sl_payload);
        put8(b, 0x02);                    /* predefined: MP4 */

        box_close(b, at);
        break;
    }
    default:
        b->err = true;
        break;
    }
}

static void write_sample_entry(buf_t *b, const moq_cmaf_packager_t *p,
                               const moq_cmaf_packager_cfg_t *cfg)
{
    size_t at = box_open(b, sample_entry_type(p->codec_kind));

    put_zero(b, 6);   /* SampleEntry reserved */
    put16(b, 1);      /* data_reference_index */

    if (p->is_video) {
        put_zero(b, 2);                    /* pre_defined */
        put_zero(b, 2);                    /* reserved */
        put_zero(b, 12);                   /* pre_defined[3] */
        put16(b, (uint16_t)cfg->width);
        put16(b, (uint16_t)cfg->height);
        put32(b, 0x00480000);              /* horizresolution: 72 dpi */
        put32(b, 0x00480000);              /* vertresolution */
        put32(b, 0);                       /* reserved */
        put16(b, 1);                       /* frame_count */
        put_zero(b, 32);                   /* compressorname */
        put16(b, 0x0018);                  /* depth: colour, no alpha */
        put16(b, 0xffff);                  /* pre_defined = -1 */
    } else {
        put_zero(b, 8);                    /* reserved[2] */
        put16(b, (uint16_t)cfg->channel_count);
        put16(b, 16);                      /* samplesize */
        put_zero(b, 2);                    /* pre_defined */
        put_zero(b, 2);                    /* reserved */
        /*
         * samplerate is 16.16 fixed point, so only the integer part is
         * expressible. RFC 7845 4.1 pins the Opus sample entry at 48000
         * whatever the mdhd timescale says.
         */
        uint32_t entry_rate = (p->codec_kind == MOQ_CMAF_CODEC_OPUS)
                                  ? 48000u : cfg->samplerate;
        put32(b, entry_rate << 16);
    }

    write_codec_config_box(b, p);
    box_close(b, at);
}

static void write_stbl(buf_t *b, const moq_cmaf_packager_t *p,
                       const moq_cmaf_packager_cfg_t *cfg)
{
    size_t at = box_open(b, FOURCC('s','t','b','l'));

    size_t stsd = box_open(b, FOURCC('s','t','s','d'));
    put_fullbox(b, 0, 0);
    put32(b, 1);                     /* entry_count */
    write_sample_entry(b, p, cfg);
    box_close(b, stsd);

    /* A fragmented track keeps its sample tables empty: every sample is
     * described by a trun instead. */
    size_t stts = box_open(b, FOURCC('s','t','t','s'));
    put_fullbox(b, 0, 0);
    put32(b, 0);
    box_close(b, stts);

    size_t stsc = box_open(b, FOURCC('s','t','s','c'));
    put_fullbox(b, 0, 0);
    put32(b, 0);
    box_close(b, stsc);

    size_t stsz = box_open(b, FOURCC('s','t','s','z'));
    put_fullbox(b, 0, 0);
    put32(b, 0);                     /* sample_size */
    put32(b, 0);                     /* sample_count */
    box_close(b, stsz);

    size_t stco = box_open(b, FOURCC('s','t','c','o'));
    put_fullbox(b, 0, 0);
    put32(b, 0);
    box_close(b, stco);

    box_close(b, at);
}

static void write_minf(buf_t *b, const moq_cmaf_packager_t *p,
                       const moq_cmaf_packager_cfg_t *cfg)
{
    size_t at = box_open(b, FOURCC('m','i','n','f'));

    if (p->is_video) {
        size_t vmhd = box_open(b, FOURCC('v','m','h','d'));
        put_fullbox(b, 0, 1);        /* flags = 1, as the spec requires */
        put16(b, 0);                 /* graphicsmode */
        put_zero(b, 6);              /* opcolor */
        box_close(b, vmhd);
    } else {
        size_t smhd = box_open(b, FOURCC('s','m','h','d'));
        put_fullbox(b, 0, 0);
        put16(b, 0);                 /* balance */
        put16(b, 0);                 /* reserved */
        box_close(b, smhd);
    }

    size_t dinf = box_open(b, FOURCC('d','i','n','f'));
    size_t dref = box_open(b, FOURCC('d','r','e','f'));
    put_fullbox(b, 0, 0);
    put32(b, 1);                     /* entry_count */
    size_t url = box_open(b, FOURCC('u','r','l',' '));
    put_fullbox(b, 0, 1);            /* flags = 1: media is self-contained */
    box_close(b, url);
    box_close(b, dref);
    box_close(b, dinf);

    write_stbl(b, p, cfg);
    box_close(b, at);
}

static void write_trak(buf_t *b, const moq_cmaf_packager_t *p,
                       const moq_cmaf_packager_cfg_t *cfg)
{
    size_t at = box_open(b, FOURCC('t','r','a','k'));

    size_t tkhd = box_open(b, FOURCC('t','k','h','d'));
    put_fullbox(b, 0, 0x000003);     /* enabled | in_movie */
    put32(b, 0);                     /* creation_time */
    put32(b, 0);                     /* modification_time */
    put32(b, p->track_id);
    put32(b, 0);                     /* reserved */
    put32(b, 0);                     /* duration: unknown, this is live */
    put_zero(b, 8);                  /* reserved[2] */
    put16(b, 0);                     /* layer */
    put16(b, 0);                     /* alternate_group */
    put16(b, p->is_video ? 0 : 0x0100); /* volume: 1.0 for audio */
    put16(b, 0);                     /* reserved */
    /* Unity matrix. */
    put32(b, 0x00010000); put32(b, 0); put32(b, 0);
    put32(b, 0); put32(b, 0x00010000); put32(b, 0);
    put32(b, 0); put32(b, 0); put32(b, 0x40000000);
    put32(b, p->is_video ? (cfg->width << 16) : 0);   /* 16.16 width */
    put32(b, p->is_video ? (cfg->height << 16) : 0);  /* 16.16 height */
    box_close(b, tkhd);

    size_t mdia = box_open(b, FOURCC('m','d','i','a'));

    size_t mdhd = box_open(b, FOURCC('m','d','h','d'));
    put_fullbox(b, 0, 0);
    put32(b, 0);                     /* creation_time */
    put32(b, 0);                     /* modification_time */
    put32(b, p->timescale);
    put32(b, 0);                     /* duration */
    put16(b, 0x55c4);                /* language: "und", packed ISO-639-2/T */
    put16(b, 0);                     /* pre_defined */
    box_close(b, mdhd);

    size_t hdlr = box_open(b, FOURCC('h','d','l','r'));
    put_fullbox(b, 0, 0);
    put32(b, 0);                     /* pre_defined */
    put32(b, p->is_video ? FOURCC('v','i','d','e') : FOURCC('s','o','u','n'));
    put_zero(b, 12);                 /* reserved */
    if (p->is_video)
        put_bytes(b, "VideoHandler", 13);   /* NUL included */
    else
        put_bytes(b, "SoundHandler", 13);
    box_close(b, hdlr);

    write_minf(b, p, cfg);
    box_close(b, mdia);
    box_close(b, at);
}

static void write_moov(buf_t *b, const moq_cmaf_packager_t *p,
                       const moq_cmaf_packager_cfg_t *cfg)
{
    size_t at = box_open(b, FOURCC('m','o','o','v'));

    size_t mvhd = box_open(b, FOURCC('m','v','h','d'));
    put_fullbox(b, 0, 0);
    put32(b, 0);                     /* creation_time */
    put32(b, 0);                     /* modification_time */
    put32(b, 1000);                  /* timescale: movie-level, milliseconds */
    put32(b, 0);                     /* duration: unknown */
    put32(b, 0x00010000);            /* rate: 1.0 */
    put16(b, 0x0100);                /* volume: 1.0 */
    put16(b, 0);                     /* reserved */
    put_zero(b, 8);                  /* reserved[2] */
    put32(b, 0x00010000); put32(b, 0); put32(b, 0);
    put32(b, 0); put32(b, 0x00010000); put32(b, 0);
    put32(b, 0); put32(b, 0); put32(b, 0x40000000);
    put_zero(b, 24);                 /* pre_defined */
    put32(b, p->track_id + 1);       /* next_track_ID */
    box_close(b, mvhd);

    write_trak(b, p, cfg);

    /*
     * mvex marks the track as fragmented. No mehd: the total duration of a
     * live stream is not known when the init segment is built.
     */
    size_t mvex = box_open(b, FOURCC('m','v','e','x'));
    size_t trex = box_open(b, FOURCC('t','r','e','x'));
    put_fullbox(b, 0, 0);
    put32(b, p->track_id);
    put32(b, 1);                     /* default_sample_description_index */
    /* No trex defaults: every trun states duration, size and flags per
     * sample, which keeps a chunk readable without its init segment. */
    put32(b, 0);                     /* default_sample_duration */
    put32(b, 0);                     /* default_sample_size */
    put32(b, 0);                     /* default_sample_flags */
    box_close(b, trex);
    box_close(b, mvex);

    box_close(b, at);
}

/* -- Config validation ----------------------------------------------- */

static moq_result_t resolve_sample_format(moq_cmaf_packager_cfg_t *cfg)
{
    if (cfg->sample_format == MOQ_CMAF_SAMPLE_AUTO) {
        cfg->sample_format = codec_is_nal(cfg->codec_kind)
                                 ? MOQ_CMAF_SAMPLE_ANNEXB
                                 : MOQ_CMAF_SAMPLE_ELEMENTARY;
        return MOQ_OK;
    }

    switch (cfg->sample_format) {
    case MOQ_CMAF_SAMPLE_ANNEXB:
    case MOQ_CMAF_SAMPLE_LENGTH_PREFIXED:
        /* Only AVC and HEVC carry NALs; nothing else has a length prefix to
         * write or start codes to strip. */
        return codec_is_nal(cfg->codec_kind) ? MOQ_OK : MOQ_ERR_INVAL;
    case MOQ_CMAF_SAMPLE_ELEMENTARY:
        return MOQ_OK;
    default:
        return MOQ_ERR_INVAL;
    }
}

static moq_result_t resolve_timing(moq_cmaf_packager_cfg_t *cfg, bool is_video)
{
    if (!is_video && cfg->timescale == 0)
        cfg->timescale = cfg->samplerate;
    if (cfg->timescale == 0 || cfg->timescale > RESCALE_MAX)
        return MOQ_ERR_INVAL;
    if (cfg->source_timebase_den > RESCALE_MAX)
        return MOQ_ERR_INVAL;
    /* Both or neither: half a timebase says nothing. */
    if ((cfg->source_timebase_num == 0) != (cfg->source_timebase_den == 0))
        return MOQ_ERR_INVAL;
    /* rescale() multiplies the two together, so their product is what has to
     * stay in range, not each factor. */
    if ((int64_t)cfg->timescale * cfg->source_timebase_num > RESCALE_MAX)
        return MOQ_ERR_INVAL;

    if (cfg->default_sample_duration != 0)
        return MOQ_OK;

    if (is_video) {
        int64_t d;
        if (cfg->fps_num == 0 || cfg->fps_den == 0 ||
            cfg->fps_num > RESCALE_MAX || cfg->fps_den > RESCALE_MAX)
            return MOQ_ERR_INVAL;
        if (!rescale(cfg->timescale, cfg->fps_den, cfg->fps_num, &d))
            return MOQ_ERR_INVAL;
        if (d <= 0 || d > 0xffffffffLL)
            return MOQ_ERR_INVAL;
        cfg->default_sample_duration = (uint32_t)d;
    } else if (cfg->codec_kind == MOQ_CMAF_CODEC_AAC) {
        cfg->default_sample_duration = aac_frame_length(cfg->codec_config);
    }
    /* Opus needs nothing here: its frame length varies per packet and is read
     * from each TOC byte at write() time. */

    return MOQ_OK;
}

static moq_result_t validate_cfg(moq_cmaf_packager_cfg_t *cfg)
{
    if (sample_entry_type(cfg->codec_kind) == 0)
        return MOQ_ERR_INVAL;
    if (!cfg->codec_config.data || cfg->codec_config.len == 0)
        return MOQ_ERR_INVAL;

    const bool is_video = codec_is_video(cfg->codec_kind);

    if (is_video) {
        /* Written into tkhd and the sample entry as 16-bit / 16.16 fields. */
        if (cfg->width == 0 || cfg->height == 0 ||
            cfg->width > 0xffff || cfg->height > 0xffff)
            return MOQ_ERR_INVAL;
    } else {
        if (cfg->samplerate == 0 || cfg->samplerate > 0xffff)
            return MOQ_ERR_INVAL;
        if (cfg->channel_count == 0 || cfg->channel_count > 0xffff)
            return MOQ_ERR_INVAL;
    }

    if (cfg->track_id == 0) cfg->track_id = 1;

    moq_result_t rc = resolve_sample_format(cfg);
    if (rc < 0) return rc;

    return resolve_timing(cfg, is_video);
}

/* -- Public API ------------------------------------------------------ */

void moq_cmaf_packager_cfg_init(moq_cmaf_packager_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = (uint32_t)sizeof(*cfg);
}

moq_result_t moq_cmaf_packager_create(const moq_alloc_t *alloc,
                                      const moq_cmaf_packager_cfg_t *cfg,
                                      moq_cmaf_packager_t **out)
{
    if (!cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;

    if (cfg->struct_size < offsetof(moq_cmaf_packager_cfg_t, avg_bitrate)
                               + sizeof(cfg->avg_bitrate))
        return MOQ_ERR_INVAL;

    if (!alloc) alloc = moq_alloc_default();
    if (!alloc->alloc || !alloc->free) return MOQ_ERR_INVAL;

    /* A local copy: validation fills in the derived fields without touching
     * the caller's struct. */
    moq_cmaf_packager_cfg_t c = *cfg;
    moq_result_t rc = validate_cfg(&c);
    if (rc < 0) return rc;

    uint8_t nal_length_size = 4;
    if (codec_is_nal(c.codec_kind)) {
        rc = nal_length_size_from_record(c.codec_kind, c.codec_config,
                                         &nal_length_size);
        if (rc < 0) return rc;
    }

    moq_cmaf_packager_t *p = alloc->alloc(sizeof(*p), alloc->ctx);
    if (!p) return MOQ_ERR_NOMEM;
    memset(p, 0, sizeof(*p));

    p->alloc = alloc;
    p->codec_kind = c.codec_kind;
    p->sample_format = c.sample_format;
    p->is_video = codec_is_video(c.codec_kind);
    p->timescale = c.timescale;
    p->track_id = c.track_id;
    p->default_duration = c.default_sample_duration;
    p->avg_bitrate = c.avg_bitrate;
    p->nal_length_size = nal_length_size;
    p->rebase = c.rebase_timestamps;
    p->src_num = (int64_t)c.timescale * (int64_t)c.source_timebase_num;
    p->src_den = (int64_t)c.source_timebase_den;
    p->init.alloc = alloc;
    p->frag.alloc = alloc;
    p->mdat.alloc = alloc;

    p->codec_config = alloc->alloc(c.codec_config.len, alloc->ctx);
    if (!p->codec_config) {
        moq_cmaf_packager_destroy(p);
        return MOQ_ERR_NOMEM;
    }
    memcpy(p->codec_config, c.codec_config.data, c.codec_config.len);
    p->codec_config_len = c.codec_config.len;

    write_ftyp(&p->init);
    write_moov(&p->init, p, &c);
    if (p->init.err) {
        moq_cmaf_packager_destroy(p);
        return MOQ_ERR_NOMEM;
    }

    *out = p;
    return MOQ_OK;
}

void moq_cmaf_packager_destroy(moq_cmaf_packager_t *p)
{
    if (!p) return;
    const moq_alloc_t *alloc = p->alloc;

    buf_free(&p->init);
    buf_free(&p->frag);
    buf_free(&p->mdat);
    if (p->codec_config)
        alloc->free(p->codec_config, p->codec_config_len, alloc->ctx);
    alloc->free(p, sizeof(*p), alloc->ctx);
}

moq_bytes_t moq_cmaf_packager_init_segment(const moq_cmaf_packager_t *p)
{
    moq_bytes_t empty = { NULL, 0 };
    if (!p) return empty;

    moq_bytes_t out = { p->init.p, p->init.len };
    return out;
}

uint32_t moq_cmaf_packager_timescale(const moq_cmaf_packager_t *p)
{
    return p ? p->timescale : 0;
}

/* Append one sample's payload to the mdat buffer, converting as configured.
 * *out_size gets the number of bytes appended. */
static moq_result_t append_sample(moq_cmaf_packager_t *p, moq_bytes_t data,
                                  uint32_t *out_size)
{
    const size_t before = p->mdat.len;

    switch (p->sample_format) {
    case MOQ_CMAF_SAMPLE_ANNEXB:
        if (!annexb_to_length_prefixed(&p->mdat, data, p->nal_length_size))
            return p->mdat.err ? MOQ_ERR_NOMEM : MOQ_ERR_PROTO;
        break;

    case MOQ_CMAF_SAMPLE_LENGTH_PREFIXED:
        if (!length_prefixes_valid(data, p->nal_length_size))
            return MOQ_ERR_PROTO;
        put_bytes(&p->mdat, data.data, data.len);
        break;

    case MOQ_CMAF_SAMPLE_ELEMENTARY: {
        size_t skip = 0;
        if (p->codec_kind == MOQ_CMAF_CODEC_AAC) {
            skip = adts_header_len(data);
            if (skip >= data.len) return MOQ_ERR_PROTO;
        }
        put_bytes(&p->mdat, data.data + skip, data.len - skip);
        break;
    }

    default:
        return MOQ_ERR_INVAL;
    }

    if (p->mdat.err) return MOQ_ERR_NOMEM;

    const size_t written = p->mdat.len - before;
    if (written == 0 || written > 0xffffffffu) return MOQ_ERR_PROTO;

    *out_size = (uint32_t)written;
    return MOQ_OK;
}

/* -- Fragment writing ------------------------------------------------ */

/* Per-sample trun record, resolved before the moof is written: which optional
 * fields the trun carries depends on how these compare to each other. */
typedef struct {
    uint32_t size;
    uint32_t duration;
    uint32_t flags;
    int32_t  composition_offset;
} sample_rec_t;

/*
 * Duration of one sample in timescale units. Opus states its frame length in
 * every packet's TOC byte and nowhere else, so it is read there. An explicit
 * per-sample duration always wins.
 */
static moq_result_t sample_duration(const moq_cmaf_packager_t *p,
                                    const moq_cmaf_packager_sample_t *s,
                                    uint32_t *out)
{
    if (s->duration != 0) {
        *out = s->duration;
        return MOQ_OK;
    }

    if (p->codec_kind == MOQ_CMAF_CODEC_OPUS) {
        const uint32_t samples48k = opus_packet_samples(s->data);
        if (samples48k == 0) return MOQ_ERR_PROTO;
        /* Opus always decodes at 48 kHz whatever the track timescale is. */
        int64_t d;
        if (!rescale(samples48k, p->timescale, 48000, &d) || d <= 0)
            return MOQ_ERR_INVAL;
        *out = (uint32_t)d;
        return MOQ_OK;
    }

    if (p->default_duration == 0) return MOQ_ERR_INVAL;
    *out = p->default_duration;
    return MOQ_OK;
}

/* tfhd optional fields. */
#define TFHD_SAMPLE_DESCRIPTION_INDEX 0x000002u
#define TFHD_DEFAULT_SAMPLE_DURATION  0x000008u
#define TFHD_DEFAULT_SAMPLE_SIZE      0x000010u
#define TFHD_DEFAULT_SAMPLE_FLAGS     0x000020u
#define TFHD_DEFAULT_BASE_IS_MOOF     0x020000u

/* trun optional fields. */
#define TRUN_DATA_OFFSET              0x000001u
#define TRUN_FIRST_SAMPLE_FLAGS       0x000004u
#define TRUN_SAMPLE_DURATION          0x000100u
#define TRUN_SAMPLE_SIZE              0x000200u
#define TRUN_SAMPLE_FLAGS             0x000400u
#define TRUN_SAMPLE_COMPOSITION_OFFSET 0x000800u

/*
 * Which optional fields the trun has to carry: only what differs from the tfhd
 * defaults, which is what the reference mp4 muxers do.
 */
static uint32_t resolve_trun_flags(const sample_rec_t *recs, size_t count,
                                   uint32_t default_duration,
                                   uint32_t default_size,
                                   uint32_t default_flags)
{
    uint32_t flags = TRUN_DATA_OFFSET;

    for (size_t i = 0; i < count; i++) {
        if (recs[i].duration != default_duration)
            flags |= TRUN_SAMPLE_DURATION;
        if (recs[i].size != default_size)
            flags |= TRUN_SAMPLE_SIZE;
        if (i > 0 && recs[i].flags != default_flags)
            flags |= TRUN_SAMPLE_FLAGS;
        if (recs[i].composition_offset != 0)
            flags |= TRUN_SAMPLE_COMPOSITION_OFFSET;
    }

    /* Only the opening sample differs: one override beats a per-sample field
     * on every sample. */
    if (!(flags & TRUN_SAMPLE_FLAGS) && recs[0].flags != default_flags)
        flags |= TRUN_FIRST_SAMPLE_FLAGS;

    return flags;
}

/*
 * moof+mdat for one chunk. The tfhd carries the defaults, so the chunk still
 * stands alone; default_sample_flags comes from the SECOND sample when there
 * is one, because a chunk normally opens on a sync sample and taking its flags
 * as the default would force every following sample to restate its own.
 */
static void write_chunk(moq_cmaf_packager_t *p, const sample_rec_t *recs,
                        size_t count, uint64_t base_dts)
{
    const uint32_t default_duration = recs[0].duration;
    const uint32_t default_size = recs[0].size;
    const uint32_t default_flags = (count > 1) ? recs[1].flags : recs[0].flags;
    const uint32_t trun_flags = resolve_trun_flags(recs, count,
                                                   default_duration,
                                                   default_size, default_flags);

    /* Version 1 makes the composition offset signed, which reordered frames
     * need; with no offsets at all the field is absent anyway. */
    uint8_t trun_version = 0;
    for (size_t i = 0; i < count; i++) {
        if (recs[i].composition_offset < 0) trun_version = 1;
    }

    buf_t *b = &p->frag;
    size_t moof = box_open(b, FOURCC('m','o','o','f'));

    size_t mfhd = box_open(b, FOURCC('m','f','h','d'));
    put_fullbox(b, 0, 0);
    put32(b, ++p->sequence_number);
    box_close(b, mfhd);

    size_t traf = box_open(b, FOURCC('t','r','a','f'));

    size_t tfhd = box_open(b, FOURCC('t','f','h','d'));
    /*
     * default-base-is-moof with no base_data_offset: the trun data_offset is
     * relative to this moof, so the chunk carries no file-absolute reference
     * and stands alone as a MoQ object payload. CMAF wants the sample
     * description index stated rather than inherited from trex.
     */
    put_fullbox(b, 0, TFHD_DEFAULT_BASE_IS_MOOF |
                      TFHD_SAMPLE_DESCRIPTION_INDEX |
                      TFHD_DEFAULT_SAMPLE_DURATION |
                      TFHD_DEFAULT_SAMPLE_SIZE |
                      TFHD_DEFAULT_SAMPLE_FLAGS);
    put32(b, p->track_id);
    put32(b, 1);                     /* sample_description_index */
    put32(b, default_duration);
    put32(b, default_size);
    put32(b, default_flags);
    box_close(b, tfhd);

    size_t tfdt = box_open(b, FOURCC('t','f','d','t'));
    put_fullbox(b, 1, 0);            /* version 1: 64-bit */
    put64(b, base_dts);
    box_close(b, tfdt);

    size_t trun = box_open(b, FOURCC('t','r','u','n'));
    put_fullbox(b, trun_version, trun_flags);
    put32(b, (uint32_t)count);
    const size_t data_offset_at = b->len;
    put32(b, 0);                     /* data_offset, patched below */
    if (trun_flags & TRUN_FIRST_SAMPLE_FLAGS)
        put32(b, recs[0].flags);

    for (size_t i = 0; i < count; i++) {
        if (trun_flags & TRUN_SAMPLE_DURATION)
            put32(b, recs[i].duration);
        if (trun_flags & TRUN_SAMPLE_SIZE)
            put32(b, recs[i].size);
        if (trun_flags & TRUN_SAMPLE_FLAGS)
            put32(b, recs[i].flags);
        if (trun_flags & TRUN_SAMPLE_COMPOSITION_OFFSET)
            put32(b, (uint32_t)recs[i].composition_offset);
    }

    box_close(b, trun);
    box_close(b, traf);
    box_close(b, moof);

    /* data_offset counts from the start of the moof, so it is the moof's own
     * size plus the mdat header, known only now. */
    if (!b->err)
        poke32(b->p + data_offset_at, (uint32_t)(b->len - moof + 8));

    size_t mdat = box_open(b, FOURCC('m','d','a','t'));
    put_bytes(b, p->mdat.p, p->mdat.len);
    box_close(b, mdat);
}

moq_result_t moq_cmaf_packager_write(moq_cmaf_packager_t *p,
                                     const moq_cmaf_packager_sample_t *samples,
                                     size_t count,
                                     moq_bytes_t *out_fragment)
{
    if (!p || !samples || !out_fragment || count == 0) return MOQ_ERR_INVAL;
    if (count > 0xffffffffu) return MOQ_ERR_INVAL;

    out_fragment->data = NULL;
    out_fragment->len = 0;

    p->mdat.len = 0;
    p->mdat.err = false;
    p->frag.len = 0;
    p->frag.err = false;

    /* Latched below on success only, so a rejected chunk cannot define the
     * timeline. */
    const int64_t origin = !p->rebase        ? 0
                         : p->have_origin    ? p->origin
                                             : samples[0].dts;

    /* Most chunks hold one sample, so the common case stays off the heap. */
    sample_rec_t inline_recs[16];
    sample_rec_t *recs = inline_recs;
    if (count > sizeof(inline_recs) / sizeof(inline_recs[0])) {
        /* The trun count check above is vacuous where size_t is 32-bit, so the
         * table size is what has to be bounded. */
        if (count > (size_t)-1 / sizeof(*recs))
            return MOQ_ERR_INVAL;
        recs = p->alloc->alloc(count * sizeof(*recs), p->alloc->ctx);
        if (!recs) return MOQ_ERR_NOMEM;
    }

    moq_result_t rc = MOQ_OK;
    for (size_t i = 0; i < count; i++) {
        const moq_cmaf_packager_sample_t *s = &samples[i];
        if (!s->data.data || s->data.len == 0) {
            rc = MOQ_ERR_INVAL;
            break;
        }

        /* The trun composition offset is a signed 32-bit field; a pts/dts gap
         * that does not fit is rejected rather than silently truncated. */
        int64_t rel_dts, rel_pts, dts, pts, cts;
        if (!sub_checked(s->dts, origin, &rel_dts) ||
            !sub_checked(s->pts, origin, &rel_pts) ||
            !rescale(rel_dts, p->src_num, p->src_den, &dts) ||
            !rescale(rel_pts, p->src_num, p->src_den, &pts) ||
            !sub_checked(pts, dts, &cts) ||
            cts < INT32_MIN || cts > INT32_MAX) {
            rc = MOQ_ERR_INVAL;
            break;
        }
        recs[i].composition_offset = (int32_t)cts;
        recs[i].flags = s->keyframe ? SAMPLE_FLAGS_SYNC : SAMPLE_FLAGS_NON_SYNC;

        rc = sample_duration(p, s, &recs[i].duration);
        if (rc < 0) break;

        rc = append_sample(p, s->data, &recs[i].size);
        if (rc < 0) break;
    }

    /* tfdt is unsigned: a negative decode time has no representation, and
     * clamping it to zero would silently shift the timeline. */
    int64_t base_dts = 0;
    if (rc >= 0) {
        int64_t rel;
        if (!sub_checked(samples[0].dts, origin, &rel) ||
            !rescale(rel, p->src_num, p->src_den, &base_dts) ||
            base_dts < 0)
            rc = MOQ_ERR_INVAL;
    }

    if (rc >= 0) {
        write_chunk(p, recs, count, (uint64_t)base_dts);
        if (p->frag.err) rc = MOQ_ERR_NOMEM;
    }

    if (recs != inline_recs)
        p->alloc->free(recs, count * sizeof(*recs), p->alloc->ctx);

    if (rc < 0) {
        p->frag.len = 0;
        return rc;
    }

    p->origin = origin;
    p->have_origin = true;

    out_fragment->data = p->frag.p;
    out_fragment->len = p->frag.len;
    return MOQ_OK;
}
