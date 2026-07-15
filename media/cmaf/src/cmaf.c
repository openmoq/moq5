#include <moq/cmaf.h>
#include <string.h>

/* -- Endian helpers -------------------------------------------------- */

static inline uint32_t rd32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16)
         | ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
}

static inline uint16_t rd16(const uint8_t *p)
{
    return (uint16_t)((p[0] << 8) | p[1]);
}

static inline uint64_t rd64(const uint8_t *p)
{
    return ((uint64_t)rd32(p) << 32) | rd32(p + 4);
}

/* -- FOURCC ---------------------------------------------------------- */

#define FOURCC(a,b,c,d) \
    (((uint32_t)(a)<<24)|((uint32_t)(b)<<16)|((uint32_t)(c)<<8)|(uint32_t)(d))

static inline uint32_t box_type(const uint8_t *p) { return rd32(p + 4); }

/* -- Box reader ------------------------------------------------------ */

typedef struct {
    const uint8_t *data;
    size_t         size;
    size_t         hdr;
} box_t;

static int read_box(box_t *out, const uint8_t *data, size_t avail)
{
    if (avail < 8) return -1;

    uint64_t size = rd32(data);
    size_t hdr = 8;

    if (size == 1) {
        if (avail < 16) return -1;
        size = rd64(data + 8);
        hdr = 16;
    } else if (size == 0) {
        size = avail;
    }

    if (size > avail || size < hdr) return -1;

    out->data = data;
    out->size = (size_t)size;
    out->hdr = hdr;
    return 0;
}

/* find_box: returns 0 and sets *out_pos on success,
 * 1 if type not found (clean), -1 on malformed box. */
static int find_box_rc(const uint8_t *data, size_t len, uint32_t type,
                        const uint8_t **out_pos)
{
    size_t pos = 0;
    while (pos + 8 <= len) {
        box_t b;
        if (read_box(&b, data + pos, len - pos) < 0) return -1;
        if (box_type(data + pos) == type) {
            *out_pos = data + pos;
            return 0;
        }
        pos += b.size;
    }
    if (pos != len) return -1;
    return 1;
}

/* find_box_nested_rc: returns 0 on success, 1 if absent, -1 on malformed. */
static int find_box_nested_rc(const uint8_t *data, size_t len,
                               const uint32_t *path, size_t depth,
                               const uint8_t **out_pos)
{
    const uint8_t *cur = data;
    size_t cur_len = len;

    for (size_t d = 0; d < depth; d++) {
        const uint8_t *found;
        int rc = find_box_rc(cur, cur_len, path[d], &found);
        if (rc != 0) return rc;

        box_t b;
        if (read_box(&b, found, cur_len - (size_t)(found - cur)) < 0)
            return -1;

        if (d == depth - 1) {
            *out_pos = found;
            return 0;
        }

        cur = found + b.hdr;
        cur_len = b.size - b.hdr;
    }
    return 1;
}

/* -- Init functions -------------------------------------------------- */

void moq_cmaf_init_info_init(moq_cmaf_init_info_t *info)
{
    if (!info) return;
    memset(info, 0, sizeof(*info));
    info->struct_size = sizeof(*info);
}

void moq_cmaf_fragment_info_init(moq_cmaf_fragment_info_t *info,
                                  moq_cmaf_sample_t *samples,
                                  size_t sample_cap)
{
    if (!info) return;
    memset(info, 0, sizeof(*info));
    info->struct_size = sizeof(*info);
    info->samples = samples;
    info->sample_cap = sample_cap;
}

/* -- parse_init ------------------------------------------------------ */

static moq_result_t parse_mdhd_timescale(const uint8_t *mdhd_box,
                                          size_t avail, uint32_t *out)
{
    box_t b;
    if (read_box(&b, mdhd_box, avail) < 0) return MOQ_ERR_PROTO;
    const uint8_t *p = mdhd_box + b.hdr;
    size_t remain = b.size - b.hdr;
    if (remain < 4) return MOQ_ERR_PROTO;

    uint8_t version = p[0];
    if (version > 1) return MOQ_ERR_PROTO;
    p += 4; remain -= 4;

    if (version == 1) {
        if (remain < 20) return MOQ_ERR_PROTO;
        *out = rd32(p + 16);
    } else {
        if (remain < 12) return MOQ_ERR_PROTO;
        *out = rd32(p + 8);
    }
    return MOQ_OK;
}

/*
 * Extract the decoder config from a sample entry's child-box region (eb/eb_len)
 * given the effective codec fourcc -- which is the sample-entry type for a clear
 * track, or the original format (sinf 'frma') for an encrypted entry. A KNOWN
 * codec whose config box is missing/malformed is MOQ_ERR_PROTO; an unknown
 * fourcc leaves codec_kind UNKNOWN and returns MOQ_OK.
 */
static moq_result_t extract_codec_config(uint32_t etype,
                                         const uint8_t *eb, size_t eb_len,
                                         moq_cmaf_init_info_t *out)
{
    if (etype == FOURCC('a','v','c','1') ||
        etype == FOURCC('a','v','c','3')) {
        const uint8_t *avcc;
        if (find_box_rc(eb, eb_len, FOURCC('a','v','c','C'), &avcc) != 0)
            return MOQ_ERR_PROTO;
        box_t cb;
        if (read_box(&cb, avcc, eb_len - (size_t)(avcc - eb)) < 0)
            return MOQ_ERR_PROTO;
        out->codec_kind = MOQ_CMAF_CODEC_AVC;
        out->codec_config.data = avcc + cb.hdr;
        out->codec_config.len = cb.size - cb.hdr;
        return MOQ_OK;

    } else if (etype == FOURCC('h','e','v','1') ||
               etype == FOURCC('h','v','c','1')) {
        const uint8_t *hvcc;
        if (find_box_rc(eb, eb_len, FOURCC('h','v','c','C'), &hvcc) != 0)
            return MOQ_ERR_PROTO;
        box_t cb;
        if (read_box(&cb, hvcc, eb_len - (size_t)(hvcc - eb)) < 0)
            return MOQ_ERR_PROTO;
        out->codec_kind = MOQ_CMAF_CODEC_HEVC;
        out->codec_config.data = hvcc + cb.hdr;
        out->codec_config.len = cb.size - cb.hdr;
        return MOQ_OK;

    } else if (etype == FOURCC('a','v','0','1')) {
        const uint8_t *av1c;
        if (find_box_rc(eb, eb_len, FOURCC('a','v','1','C'), &av1c) != 0)
            return MOQ_ERR_PROTO;
        box_t cb;
        if (read_box(&cb, av1c, eb_len - (size_t)(av1c - eb)) < 0)
            return MOQ_ERR_PROTO;
        out->codec_kind = MOQ_CMAF_CODEC_AV1;
        out->codec_config.data = av1c + cb.hdr;
        out->codec_config.len = cb.size - cb.hdr;
        return MOQ_OK;

    } else if (etype == FOURCC('m','p','4','a')) {
        const uint8_t *esds;
        if (find_box_rc(eb, eb_len, FOURCC('e','s','d','s'), &esds) != 0)
            return MOQ_ERR_PROTO;
        box_t eb2;
        if (read_box(&eb2, esds, eb_len - (size_t)(esds - eb)) < 0)
            return MOQ_ERR_PROTO;
        if (eb2.size < eb2.hdr + 4) return MOQ_ERR_PROTO;

        const uint8_t *esd = esds + eb2.hdr + 4;
        size_t esd_len = eb2.size - eb2.hdr - 4;

        size_t pos = 0;
        while (pos + 2 < esd_len) {
            uint8_t tag = esd[pos++];
            uint32_t sz = 0;
            for (int i = 0; i < 4 && pos < esd_len; i++) {
                uint8_t byte = esd[pos++];
                sz = (sz << 7) | (byte & 0x7F);
                if (!(byte & 0x80)) break;
            }
            if (tag == 0x05 && sz > 0 && pos + sz <= esd_len) {
                out->codec_kind = MOQ_CMAF_CODEC_AAC;
                out->codec_config.data = esd + pos;
                out->codec_config.len = sz;
                return MOQ_OK;
            }
            if (tag == 0x03) {
                if (pos + 3 > esd_len) return MOQ_ERR_PROTO;
                pos += 3;
            } else if (tag == 0x04) {
                if (pos + 13 > esd_len) return MOQ_ERR_PROTO;
                pos += 13;
            } else {
                pos += sz;
            }
        }
        return MOQ_ERR_PROTO;

    } else if (etype == FOURCC('O','p','u','s')) {
        const uint8_t *dops;
        if (find_box_rc(eb, eb_len, FOURCC('d','O','p','s'), &dops) != 0)
            return MOQ_ERR_PROTO;
        box_t cb;
        if (read_box(&cb, dops, eb_len - (size_t)(dops - eb)) < 0)
            return MOQ_ERR_PROTO;
        out->codec_kind = MOQ_CMAF_CODEC_OPUS;
        out->codec_config.data = dops + cb.hdr;
        out->codec_config.len = cb.size - cb.hdr;
        return MOQ_OK;
    }

    out->codec_kind = MOQ_CMAF_CODEC_UNKNOWN;
    return MOQ_OK;
}

/*
 * Parse the CENC protection boxes within an encrypted sample entry's child
 * region (eb/eb_len): sinf( frma + schm + schi(tenc) ) per CMSF §4.2 / [CENC].
 * On success sets out->has_cenc, scheme, default_is_protected,
 * default_per_sample_iv_size, default_kid, and returns the original sample-entry
 * fourcc in *out_frma. The entry is explicitly encrypted, so every required box
 * MUST be present and well-formed -- missing/short -> MOQ_ERR_PROTO.
 */
static moq_result_t parse_cenc_sinf(const uint8_t *eb, size_t eb_len,
                                    moq_cmaf_init_info_t *out,
                                    uint32_t *out_frma)
{
    const uint8_t *sinf;
    if (find_box_rc(eb, eb_len, FOURCC('s','i','n','f'), &sinf) != 0)
        return MOQ_ERR_PROTO;
    box_t sinf_box;
    if (read_box(&sinf_box, sinf, eb_len - (size_t)(sinf - eb)) < 0)
        return MOQ_ERR_PROTO;
    const uint8_t *sb = sinf + sinf_box.hdr;
    size_t sb_len = sinf_box.size - sinf_box.hdr;

    /* frma: original format -- a 4cc payload word. */
    const uint8_t *frma;
    if (find_box_rc(sb, sb_len, FOURCC('f','r','m','a'), &frma) != 0)
        return MOQ_ERR_PROTO;
    box_t frma_box;
    if (read_box(&frma_box, frma, sb_len - (size_t)(frma - sb)) < 0)
        return MOQ_ERR_PROTO;
    if (frma_box.size < frma_box.hdr + 4) return MOQ_ERR_PROTO;
    *out_frma = rd32(frma + frma_box.hdr);

    /* schm: FullBox(4) + scheme_type(4) + scheme_version(4) -> body >= 12. */
    const uint8_t *schm;
    if (find_box_rc(sb, sb_len, FOURCC('s','c','h','m'), &schm) != 0)
        return MOQ_ERR_PROTO;
    box_t schm_box;
    if (read_box(&schm_box, schm, sb_len - (size_t)(schm - sb)) < 0)
        return MOQ_ERR_PROTO;
    if (schm_box.size < schm_box.hdr + 12) return MOQ_ERR_PROTO;
    out->scheme.data = schm + schm_box.hdr + 4;   /* scheme_type 4cc */
    out->scheme.len = 4;

    /* schi -> tenc. */
    const uint32_t tenc_path[] = {
        FOURCC('s','c','h','i'), FOURCC('t','e','n','c'),
    };
    const uint8_t *tenc;
    if (find_box_nested_rc(sb, sb_len, tenc_path, 2, &tenc) != 0)
        return MOQ_ERR_PROTO;
    box_t tenc_box;
    if (read_box(&tenc_box, tenc, sb_len - (size_t)(tenc - sb)) < 0)
        return MOQ_ERR_PROTO;
    const uint8_t *tp = tenc + tenc_box.hdr;
    size_t tlen = tenc_box.size - tenc_box.hdr;
    /* FullBox(4) + reserved/pattern(1) + isProtected(1) + iv_size(1)
     * + default_KID(16) = 23 byte minimum; trailing (constant-IV) data
     * is permitted and ignored. */
    if (tlen < 23) return MOQ_ERR_PROTO;
    out->default_is_protected = tp[5];
    out->default_per_sample_iv_size = tp[6];
    out->default_kid.data = tp + 7;
    out->default_kid.len = 16;

    out->has_cenc = true;
    return MOQ_OK;
}

static moq_result_t parse_stsd_entry(const uint8_t *stsd_box,
                                      size_t avail,
                                      moq_cmaf_init_info_t *out)
{
    box_t sb;
    if (read_box(&sb, stsd_box, avail) < 0) return MOQ_ERR_PROTO;
    if (sb.size < sb.hdr + 8) return MOQ_ERR_PROTO;

    const uint8_t *body = stsd_box + sb.hdr + 8;
    size_t body_len = sb.size - sb.hdr - 8;

    box_t entry;
    if (read_box(&entry, body, body_len) < 0) return MOQ_ERR_PROTO;

    /* Only the first sample entry is parsed (multiple entries are not
     * iterated -- the established behavior). */
    uint32_t etype = box_type(body);

    bool is_visual = etype == FOURCC('a','v','c','1') ||
                     etype == FOURCC('a','v','c','3') ||
                     etype == FOURCC('h','e','v','1') ||
                     etype == FOURCC('h','v','c','1') ||
                     etype == FOURCC('a','v','0','1') ||
                     etype == FOURCC('e','n','c','v');
    bool is_audio  = etype == FOURCC('m','p','4','a') ||
                     etype == FOURCC('O','p','u','s') ||
                     etype == FOURCC('e','n','c','a');

    const uint8_t *eb;
    size_t eb_len;

    if (is_visual) {
        /* VisualSampleEntry: 78-byte fixed header before child boxes. The
         * encrypted entry (encv) shares this exact layout. */
        if (entry.size < entry.hdr + 78) return MOQ_ERR_PROTO;
        eb = body + entry.hdr + 78;
        eb_len = entry.size - entry.hdr - 78;
        if (entry.size >= entry.hdr + 28) {
            out->width = rd16(body + entry.hdr + 24);
            out->height = rd16(body + entry.hdr + 26);
        }
    } else if (is_audio) {
        /* AudioSampleEntry: 28-byte fixed header (enca shares it). */
        if (entry.size < entry.hdr + 28) return MOQ_ERR_PROTO;
        eb = body + entry.hdr + 28;
        eb_len = entry.size - entry.hdr - 28;
        out->samplerate = rd16(body + entry.hdr + 24);
        out->channel_count = rd16(body + entry.hdr + 8);
    } else {
        out->codec_kind = MOQ_CMAF_CODEC_UNKNOWN;
        return MOQ_OK;
    }

    /* For an encrypted entry, parse the CENC boxes and recover the original
     * codec fourcc from sinf 'frma' so the decoder config still resolves. */
    uint32_t effective = etype;
    if (etype == FOURCC('e','n','c','v') || etype == FOURCC('e','n','c','a')) {
        uint32_t frma = 0;
        moq_result_t crc = parse_cenc_sinf(eb, eb_len, out, &frma);
        if (crc < 0) return crc;
        effective = frma;
    }

    return extract_codec_config(effective, eb, eb_len, out);
}

moq_result_t moq_cmaf_parse_init(moq_bytes_t init_segment,
                                  moq_cmaf_init_info_t *out)
{
    if (!init_segment.data || !out) return MOQ_ERR_INVAL;
    moq_cmaf_init_info_init(out);

    const uint32_t trak_path[] = {
        FOURCC('m','o','o','v'), FOURCC('t','r','a','k'),
    };
    const uint8_t *trak;
    int trc = find_box_nested_rc(init_segment.data, init_segment.len,
                                  trak_path, 2, &trak);
    if (trc != 0) return MOQ_ERR_PROTO;

    box_t trak_box;
    if (read_box(&trak_box, trak,
        init_segment.len - (size_t)(trak - init_segment.data)) < 0)
        return MOQ_ERR_PROTO;

    const uint8_t *trak_body = trak + trak_box.hdr;
    size_t trak_len = trak_box.size - trak_box.hdr;

    /* track_ID from tkhd (optional; leaves 0 when absent/malformed). The
     * CMSF §3.3 object-vs-init track_ID match needs this. */
    const uint8_t *tkhd;
    if (find_box_rc(trak_body, trak_len, FOURCC('t','k','h','d'), &tkhd) == 0) {
        box_t kb;
        if (read_box(&kb, tkhd, trak_len - (size_t)(tkhd - trak_body)) == 0) {
            const uint8_t *p = tkhd + kb.hdr;
            size_t r = kb.size - kb.hdr;
            if (r >= 4) {
                /* fullbox: v0 -> creation(4) modification(4) track_ID(4);
                 *          v1 -> creation(8) modification(8) track_ID(4). */
                size_t tid_off = (p[0] == 1) ? 20 : 12;
                if (r >= tid_off + 4)
                    out->track_id = rd32(p + tid_off);
            }
        }
    }

    /* mdia within the trak. */
    const uint8_t *mdia;
    int frc = find_box_rc(trak_body, trak_len, FOURCC('m','d','i','a'), &mdia);
    if (frc != 0) return MOQ_ERR_PROTO;

    box_t mdia_box;
    if (read_box(&mdia_box, mdia,
        trak_len - (size_t)(mdia - trak_body)) < 0)
        return MOQ_ERR_PROTO;

    const uint8_t *mdia_body = mdia + mdia_box.hdr;
    size_t mdia_len = mdia_box.size - mdia_box.hdr;

    /* Timescale from mdhd — required. */
    const uint8_t *mdhd;
    int mdhd_rc = find_box_rc(mdia_body, mdia_len,
                               FOURCC('m','d','h','d'), &mdhd);
    if (mdhd_rc != 0) return MOQ_ERR_PROTO;
    moq_result_t rc = parse_mdhd_timescale(mdhd,
        mdia_len - (size_t)(mdhd - mdia_body), &out->timescale);
    if (rc < 0) return rc;

    /* Walk deeper: minf → stbl → stsd. */
    const uint8_t *minf;
    if (find_box_rc(mdia_body, mdia_len, FOURCC('m','i','n','f'), &minf) != 0)
        return MOQ_ERR_PROTO;
    box_t minf_box;
    if (read_box(&minf_box, minf, mdia_len - (size_t)(minf - mdia_body)) < 0)
        return MOQ_ERR_PROTO;

    const uint8_t *stbl;
    if (find_box_rc(minf + minf_box.hdr, minf_box.size - minf_box.hdr,
                     FOURCC('s','t','b','l'), &stbl) != 0)
        return MOQ_ERR_PROTO;
    box_t stbl_box;
    if (read_box(&stbl_box, stbl,
        (minf_box.size - minf_box.hdr) - (size_t)(stbl - (minf + minf_box.hdr))) < 0)
        return MOQ_ERR_PROTO;

    const uint8_t *stsd;
    if (find_box_rc(stbl + stbl_box.hdr, stbl_box.size - stbl_box.hdr,
                     FOURCC('s','t','s','d'), &stsd) != 0)
        return MOQ_ERR_PROTO;

    return parse_stsd_entry(stsd,
        (stbl_box.size - stbl_box.hdr) - (size_t)(stsd - (stbl + stbl_box.hdr)),
        out);
}

/* -- parse_fragment -------------------------------------------------- */

static moq_result_t parse_tfhd(const uint8_t *box, size_t avail,
                                moq_cmaf_fragment_info_t *out)
{
    box_t b;
    if (read_box(&b, box, avail) < 0) return MOQ_ERR_PROTO;
    if (b.size < b.hdr + 8) return MOQ_ERR_PROTO;
    const uint8_t *p = box + b.hdr;
    size_t r = b.size - b.hdr;
    if (p[0] != 0) return MOQ_ERR_PROTO;
    uint32_t fl = (p[1] << 16) | (p[2] << 8) | p[3];
    out->track_id = rd32(p + 4);   /* track_ID is mandatory in tfhd */
    p += 8; r -= 8;
    if ((fl & 0x01)) { if (r < 8) return MOQ_ERR_PROTO; p += 8; r -= 8; }
    if ((fl & 0x02)) { if (r < 4) return MOQ_ERR_PROTO; p += 4; r -= 4; }
    if ((fl & 0x08)) { if (r < 4) return MOQ_ERR_PROTO; out->default_sample_duration = rd32(p); p += 4; r -= 4; }
    if ((fl & 0x10)) { if (r < 4) return MOQ_ERR_PROTO; out->default_sample_size = rd32(p); p += 4; r -= 4; }
    if ((fl & 0x20)) { if (r < 4) return MOQ_ERR_PROTO; out->default_sample_flags = rd32(p); }
    return MOQ_OK;
}

static moq_result_t parse_tfdt(const uint8_t *box, size_t avail,
                                moq_cmaf_fragment_info_t *out)
{
    box_t b;
    if (read_box(&b, box, avail) < 0) return MOQ_ERR_PROTO;
    if (b.size < b.hdr + 4) return MOQ_ERR_PROTO;
    const uint8_t *p = box + b.hdr;
    size_t r = b.size - b.hdr;
    uint8_t ver = p[0];
    if (ver > 1) return MOQ_ERR_PROTO;
    p += 4; r -= 4;
    if (ver == 1) {
        if (r < 8) return MOQ_ERR_PROTO;
        out->base_decode_time = rd64(p);
    } else {
        if (r < 4) return MOQ_ERR_PROTO;
        out->base_decode_time = rd32(p);
    }
    out->has_base_decode_time = true;
    return MOQ_OK;
}

static moq_result_t parse_trun(const uint8_t *box, size_t avail,
                                size_t fragment_len,
                                moq_cmaf_fragment_info_t *out,
                                uint32_t *required_count,
                                bool *need_buffer)
{
    *required_count = 0;
    *need_buffer = false;
    box_t b;
    if (read_box(&b, box, avail) < 0) return MOQ_ERR_PROTO;
    if (b.size < b.hdr + 8) return MOQ_ERR_PROTO;
    const uint8_t *p = box + b.hdr;
    size_t r = b.size - b.hdr;
    uint8_t ver = p[0];
    if (ver > 1) return MOQ_ERR_PROTO;
    uint32_t fl = (p[1] << 16) | (p[2] << 8) | p[3];
    uint32_t count = rd32(p + 4);
    p += 8; r -= 8;
    if ((fl & 0x01)) { if (r < 4) return MOQ_ERR_PROTO; p += 4; r -= 4; }
    if ((fl & 0x04)) { if (r < 4) return MOQ_ERR_PROTO; p += 4; r -= 4; }

    if (count == 0) return MOQ_ERR_PROTO;

    /* Validate the sample table is structurally possible BEFORE exposing
     * count as a required allocation size. A per-sample record is the sum of
     * whichever optional fields the flags declare; the trun must carry
     * count*per_sample bytes for them. A malformed/truncated trun that claims
     * a huge count therefore fails as PROTO and never surfaces an attacker-
     * controlled allocation size via out->sample_count. */
    size_t per_sample = 0;
    if (fl & 0x100) per_sample += 4;   /* sample_duration */
    if (fl & 0x200) per_sample += 4;   /* sample_size */
    if (fl & 0x400) per_sample += 4;   /* sample_flags */
    if (fl & 0x800) per_sample += 4;   /* sample_composition_time_offset */

    /* Upper-bound count by the whole fragment: even an all-defaults trun
     * (per_sample == 0, carrying no per-sample bytes) cannot legitimately
     * describe more samples than the fragment has bytes. This bounds the
     * per_sample==0 case the trun-internal width check cannot, and bounds the
     * per-sample loop below. */
    if ((uint64_t)count > (uint64_t)fragment_len)
        return MOQ_ERR_PROTO;
    if (per_sample > 0 &&
        (uint64_t)count * (uint64_t)per_sample > (uint64_t)r)
        return MOQ_ERR_PROTO;

    /* Structurally sound: report the trusted required count to the caller
     * WITHOUT stamping out->sample_count. parse_fragment() commits it to the
     * output only after the whole fragment validates, so a fragment that
     * fails a later check (missing mdat, bad ordering, trailing bytes) returns
     * PROTO with out->sample_count still 0 -- no allocation-size leak. */
    *required_count = count;

    /* Caller buffer too small (or absent): defer the BUFFER signal so
     * parse_fragment() can still reject a malformed remainder (missing mdat,
     * bad moof->mdat ordering, trailing bytes) as PROTO. Only a fully valid
     * fragment whose count exceeds sample_cap returns BUFFER. */
    if (!out->samples || count > out->sample_cap) {
        *need_buffer = true;
        return MOQ_OK;
    }

    for (uint32_t i = 0; i < count; i++) {
        moq_cmaf_sample_t *s = &out->samples[i];
        s->duration = out->default_sample_duration;
        s->size = out->default_sample_size;
        s->flags = out->default_sample_flags;
        s->composition_offset = 0;
        if ((fl & 0x100)) { if (r < 4) return MOQ_ERR_PROTO; s->duration = rd32(p); p += 4; r -= 4; }
        if ((fl & 0x200)) { if (r < 4) return MOQ_ERR_PROTO; s->size = rd32(p); p += 4; r -= 4; }
        if ((fl & 0x400)) { if (r < 4) return MOQ_ERR_PROTO; s->flags = rd32(p); p += 4; r -= 4; }
        if ((fl & 0x800)) {
            if (r < 4) return MOQ_ERR_PROTO;
            if (ver == 0)
                s->composition_offset = (int32_t)(uint32_t)rd32(p);
            else
                s->composition_offset = (int32_t)rd32(p);
            p += 4; r -= 4;
        }
    }
    return MOQ_OK;
}

moq_result_t moq_cmaf_parse_fragment(moq_bytes_t fragment,
                                      moq_cmaf_fragment_info_t *out)
{
    if (!fragment.data || !out) return MOQ_ERR_INVAL;

    out->sample_count = 0;
    out->has_base_decode_time = false;
    out->base_decode_time = 0;
    out->default_sample_duration = 0;
    out->default_sample_size = 0;
    out->default_sample_flags = 0;
    out->mdat = (moq_bytes_t){ NULL, 0 };
    out->track_id = 0;
    out->chunk_count = 0;

    bool got_trun = false;
    bool got_mdat = false;
    bool need_sample_buffer = false;   /* trun deferred the BUFFER signal */
    uint32_t required_count = 0;       /* trun's count, committed only on success */
    bool parsed_first = false;   /* sample table / mdat taken from the first chunk */
    bool expect_moof = true;     /* enforce successive moof -> mdat chunk ordering */
    uint32_t chunks = 0;
    size_t pos = 0;

    while (pos + 8 <= fragment.len) {
        box_t b;
        if (read_box(&b, fragment.data + pos, fragment.len - pos) < 0)
            return MOQ_ERR_PROTO;

        uint32_t type = box_type(fragment.data + pos);

        if (expect_moof) {
            /* A CMAF chunk starts with a moof, but ISO/IEC 23000-19 permits
             * prft and emsg boxes ahead of it (and a fragment may lead with
             * styp/sidx). They are metadata, not chunk structure -- skip
             * them rather than reject the object, for interop with real
             * publishers that emit them (ffmpeg -write_prft; moqtail's
             * testsrc prefixes every chunk with a prft latency anchor).
             * Anything else here (mdat first, a second moof, free, ...) is
             * still not a successive moof+mdat chunk. */
            if (type == FOURCC('p','r','f','t') ||
                type == FOURCC('e','m','s','g') ||
                type == FOURCC('s','t','y','p') ||
                type == FOURCC('s','i','d','x')) {
                pos += b.size;
                continue;
            }
            if (type != FOURCC('m','o','o','f'))
                return MOQ_ERR_PROTO;

            if (!parsed_first) {   /* normalize sample table / mdat from chunk 1 */
                parsed_first = true;
                const uint8_t *mb = fragment.data + pos + b.hdr;
                size_t ml = b.size - b.hdr;

                const uint8_t *traf;
                int traf_rc = find_box_rc(mb, ml, FOURCC('t','r','a','f'), &traf);
                if (traf_rc < 0) return MOQ_ERR_PROTO;
                if (traf_rc == 0) {
                    box_t tb;
                    if (read_box(&tb, traf, ml - (size_t)(traf - mb)) < 0)
                        return MOQ_ERR_PROTO;

                    const uint8_t *tb_body = traf + tb.hdr;
                    size_t tb_len = tb.size - tb.hdr;

                    const uint8_t *tfhd_p;
                    int tfhd_rc = find_box_rc(tb_body, tb_len,
                        FOURCC('t','f','h','d'), &tfhd_p);
                    if (tfhd_rc < 0) return MOQ_ERR_PROTO;
                    if (tfhd_rc == 0) {
                        moq_result_t rc = parse_tfhd(tfhd_p,
                            tb_len - (size_t)(tfhd_p - tb_body), out);
                        if (rc < 0) return rc;
                    }

                    const uint8_t *tfdt_p;
                    int tfdt_rc = find_box_rc(tb_body, tb_len,
                        FOURCC('t','f','d','t'), &tfdt_p);
                    if (tfdt_rc < 0) return MOQ_ERR_PROTO;
                    if (tfdt_rc == 0) {
                        moq_result_t rc = parse_tfdt(tfdt_p,
                            tb_len - (size_t)(tfdt_p - tb_body), out);
                        if (rc < 0) return rc;
                    }

                    const uint8_t *trun_p;
                    int trun_rc = find_box_rc(tb_body, tb_len,
                        FOURCC('t','r','u','n'), &trun_p);
                    if (trun_rc < 0) return MOQ_ERR_PROTO;
                    if (trun_rc == 0) {
                        moq_result_t rc = parse_trun(trun_p,
                            tb_len - (size_t)(trun_p - tb_body),
                            fragment.len, out, &required_count,
                            &need_sample_buffer);
                        if (rc < 0) return rc;
                        got_trun = true;
                    }
                }
            }
            expect_moof = false;

        } else {
            if (type != FOURCC('m','d','a','t'))
                return MOQ_ERR_PROTO;   /* moof not followed by mdat */
            if (!got_mdat) {            /* first chunk's mdat */
                out->mdat.data = fragment.data + pos + b.hdr;
                out->mdat.len = b.size - b.hdr;
                got_mdat = true;
            }
            chunks++;
            expect_moof = true;
        }

        pos += b.size;
    }

    if (pos != fragment.len)
        return MOQ_ERR_PROTO;
    if (!expect_moof)
        return MOQ_ERR_PROTO;   /* trailing moof with no mdat */
    out->chunk_count = chunks;
    if (!got_trun || required_count == 0)
        return MOQ_ERR_PROTO;
    if (!got_mdat)
        return MOQ_ERR_PROTO;
    /* Fragment is fully structurally valid: commit the required count now (it
     * stays 0 on every PROTO exit above). Then surface a sample-buffer resize
     * request -- a valid fragment whose sample count exceeds sample_cap returns
     * BUFFER with out->sample_count set to the required count. */
    out->sample_count = required_count;
    if (need_sample_buffer)
        return MOQ_ERR_BUFFER;
    return MOQ_OK;
}

/* -- SAP classification + object validation (CMSF §3.3) -------------- */

moq_sap_type_t moq_cmaf_sap_from_sample_flags(uint32_t flags)
{
    bool non_sync = (flags & 0x00010000u) != 0;
    uint32_t depends_on = (flags >> 24) & 0x3u;
    if (!non_sync)
        return MOQ_SAP_UNKNOWN;   /* sync sample: closed-GOP SAP type 1 or 2 */
    if (depends_on == 1)
        return MOQ_SAP_NONE;      /* depends on others: P/B frame, not a SAP */
    if (depends_on == 2)
        return MOQ_SAP_UNKNOWN;   /* independent but non-sync: maybe open-GOP SAP-3 */
    return MOQ_SAP_NONE;          /* non-sync, deps unknown: treat as non-SAP */
}

void moq_cmaf_object_report_init(moq_cmaf_object_report_t *r)
{
    if (!r) return;
    memset(r, 0, sizeof(*r));
    r->struct_size = sizeof(*r);
    r->sap_type = MOQ_SAP_UNKNOWN;
    r->reason = MOQ_CMAF_OK;
}

moq_result_t moq_cmaf_validate_object(const moq_cmaf_init_info_t *init,
                                      moq_bytes_t object,
                                      moq_cmaf_object_report_t *out)
{
    if (!object.data || !out) return MOQ_ERR_INVAL;
    moq_cmaf_object_report_init(out);

    uint32_t chunks = 0;
    uint32_t track_id = 0;
    bool have_tid = false;
    bool expect_moof = true;   /* strict alternation: moof, mdat, moof, mdat... */
    size_t pos = 0;
    size_t chunk_start = 0;    /* offset of the current chunk's moof */
    moq_cmaf_validity_t reason = MOQ_CMAF_OK;
    moq_cmaf_sample_t sbuf[512];

    while (pos + 8 <= object.len) {
        box_t b;
        if (read_box(&b, object.data + pos, object.len - pos) < 0) {
            reason = MOQ_CMAF_ERR_MALFORMED; break;
        }
        uint32_t type = box_type(object.data + pos);

        if (expect_moof) {
            /* A chunk must START with a moof, but ISO/IEC 23000-19 permits
             * prft/emsg (and a leading styp/sidx) ahead of it -- skip those
             * (see the matching skip in moq_cmaf_parse_fragment). Anything
             * else (mdat first, a second moof, free, ...) breaks the
             * successive-chunk rule. */
            if (type == FOURCC('p','r','f','t') ||
                type == FOURCC('e','m','s','g') ||
                type == FOURCC('s','t','y','p') ||
                type == FOURCC('s','i','d','x')) {
                pos += b.size;
                continue;
            }
            if (type != FOURCC('m','o','o','f')) {
                reason = MOQ_CMAF_ERR_MALFORMED; break;
            }
            chunk_start = pos;
            const uint8_t *mb = object.data + pos + b.hdr;
            size_t ml = b.size - b.hdr;

            const uint8_t *mfhd;
            int mr = find_box_rc(mb, ml, FOURCC('m','f','h','d'), &mfhd);
            if (mr < 0) { reason = MOQ_CMAF_ERR_MALFORMED; break; }
            if (mr > 0) { reason = MOQ_CMAF_ERR_MISSING_MFHD; break; }

            const uint8_t *traf;
            int tr = find_box_rc(mb, ml, FOURCC('t','r','a','f'), &traf);
            if (tr < 0) { reason = MOQ_CMAF_ERR_MALFORMED; break; }
            if (tr > 0) { reason = MOQ_CMAF_ERR_MALFORMED; break; } /* moof without traf */

            box_t tb;
            if (read_box(&tb, traf, ml - (size_t)(traf - mb)) < 0) {
                reason = MOQ_CMAF_ERR_MALFORMED; break;
            }

            /* A second traf in the same moof carries more than one track. */
            const uint8_t *after = traf + tb.size;
            size_t after_len = ml - (size_t)(after - mb);
            const uint8_t *traf2;
            int tr2 = find_box_rc(after, after_len, FOURCC('t','r','a','f'), &traf2);
            if (tr2 == 0) { reason = MOQ_CMAF_ERR_MULTI_TRACK; break; }
            if (tr2 < 0) { reason = MOQ_CMAF_ERR_MALFORMED; break; }

            const uint8_t *tb_body = traf + tb.hdr;
            size_t tb_len = tb.size - tb.hdr;
            const uint8_t *tfhd;
            int fr = find_box_rc(tb_body, tb_len, FOURCC('t','f','h','d'), &tfhd);
            if (fr != 0) { reason = MOQ_CMAF_ERR_MALFORMED; break; }
            box_t fb;
            if (read_box(&fb, tfhd, tb_len - (size_t)(tfhd - tb_body)) < 0
                || fb.size < fb.hdr + 8) {
                reason = MOQ_CMAF_ERR_MALFORMED; break;
            }
            uint32_t tid = rd32(tfhd + fb.hdr + 4); /* version/flags(4) then track_ID */
            if (!have_tid) { track_id = tid; have_tid = true; }
            else if (tid != track_id) { reason = MOQ_CMAF_ERR_MULTI_TRACK; break; }

            expect_moof = false;   /* the next box must be this chunk's mdat */
        } else {
            if (type != FOURCC('m','d','a','t')) {
                reason = MOQ_CMAF_ERR_MALFORMED; break;  /* moof not followed by mdat */
            }

            /* Every completed chunk must FULLY parse to a non-empty sample
             * table -- not just carry a trun box. Parse this chunk's
             * [moof..mdat] slice; the first chunk also yields the object's SAP.
             *
             * A MOQ_ERR_BUFFER means the trun declared more samples than this
             * validator's local buffer holds, so parse_trun returns before
             * reading any per-sample field -- sizes, flags, composition offsets,
             * and the first-sample SAP all go unvalidated. An unparsed sample
             * table is NOT a validated one, so fail closed rather than treat the
             * incomplete parse as success (the bypass this guards against). */
            moq_bytes_t chunk = { object.data + chunk_start,
                                  (pos + b.size) - chunk_start };
            moq_cmaf_fragment_info_t frag;
            moq_cmaf_fragment_info_init(&frag, sbuf, 512);
            moq_result_t pr = moq_cmaf_parse_fragment(chunk, &frag);
            if (pr != MOQ_OK || frag.sample_count == 0) {
                reason = MOQ_CMAF_ERR_NO_SAMPLES; break;
            }
            if (chunks == 0) {
                out->starts_with_sync =
                    (frag.samples[0].flags & 0x00010000u) == 0;
                out->sap_type =
                    moq_cmaf_sap_from_sample_flags(frag.samples[0].flags);
            }

            chunks++;
            expect_moof = true;
        }

        pos += b.size;
    }

    if (reason == MOQ_CMAF_OK && pos != object.len)
        reason = MOQ_CMAF_ERR_MALFORMED;
    if (reason == MOQ_CMAF_OK && !expect_moof)
        reason = MOQ_CMAF_ERR_MALFORMED;   /* trailing moof with no mdat */
    if (reason == MOQ_CMAF_OK && chunks == 0)
        reason = MOQ_CMAF_ERR_NO_CHUNK;
    if (reason == MOQ_CMAF_OK && init && init->track_id != 0 &&
        have_tid && track_id != init->track_id)
        reason = MOQ_CMAF_ERR_TRACK_ID_MISMATCH;

    out->chunk_count = chunks;
    out->track_id = have_tid ? track_id : 0;
    out->reason = reason;
    out->valid = (reason == MOQ_CMAF_OK);
    return out->valid ? MOQ_OK : MOQ_ERR_PROTO;
}
