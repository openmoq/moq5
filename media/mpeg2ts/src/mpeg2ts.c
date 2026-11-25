#include <moq/mpeg2ts.h>

#include <stddef.h>
#include <string.h>

#include "mpeg2ts_internal.h"

/*
 * Field layouts follow ISO/IEC 13818-1: transport packet 2.4.3.2, adaptation
 * field 2.4.3.4-5, PES packet 2.4.3.6-7, PSI sections 2.4.4 (PAT 2.4.4.3,
 * CAT 2.4.4.6, PMT 2.4.4.8), CA_descriptor 2.6.16, CRC Annex A. Packaging
 * rules follow draft-gregoire-moq-msfts; sections are named where a rule
 * comes from.
 */

/* Size of a sized input struct as first published: through its last field. */
#define V0_END(type, field) (offsetof(type, field) + sizeof(((type *)0)->field))
#define RULES_V0      MOQ_MPEG2TS_OBJECT_RULES_V0_SIZE
#define INSPECT_V0    MOQ_MPEG2TS_INSPECT_CFG_V0_SIZE
#define RX_CFG_V0     MOQ_MPEG2TS_RECEIVER_CFG_V0_SIZE
#define FILTER_CFG_V0 MOQ_MPEG2TS_PROGRAM_FILTER_CFG_V0_SIZE
#define PZ_CFG_V0     MOQ_MPEG2TS_PACKETIZER_CFG_V0_SIZE

/* Each public floor names the end of the field it is published through. */
_Static_assert(MOQ_MPEG2TS_OBJECT_RULES_V0_SIZE ==
                   V0_END(moq_mpeg2ts_object_rules_t, es_pid), "rules floor");
_Static_assert(MOQ_MPEG2TS_INSPECT_CFG_V0_SIZE ==
                   V0_END(moq_mpeg2ts_inspect_cfg_t, pmt), "inspect cfg floor");
_Static_assert(MOQ_MPEG2TS_RECEIVER_CFG_V0_SIZE ==
                   V0_END(moq_mpeg2ts_receiver_cfg_t, reorder_window_us),
               "receiver cfg floor");
_Static_assert(MOQ_MPEG2TS_PROGRAM_FILTER_CFG_V0_SIZE ==
                   V0_END(moq_mpeg2ts_program_filter_cfg_t, si_pid_count),
               "filter cfg floor");
_Static_assert(MOQ_MPEG2TS_PACKETIZER_CFG_V0_SIZE ==
                   V0_END(moq_mpeg2ts_packetizer_cfg_t, ctx),
               "packetizer cfg floor");
_Static_assert(MOQ_MPEG2TS_OBJECT_REPORT_V0_SIZE ==
                   V0_END(moq_mpeg2ts_object_report_t, bad_packet),
               "report floor");
_Static_assert(MOQ_MPEG2TS_OBJECT_INFO_V0_SIZE ==
                   V0_END(moq_mpeg2ts_object_info_t, pcr_discontinuity_index),
               "info floor");
_Static_assert(MOQ_MPEG2TS_RECEIVE_RESULT_V0_SIZE ==
                   V0_END(moq_mpeg2ts_receive_result_t, random_access),
               "result floor");
_Static_assert(MOQ_MPEG2TS_DELIVERY_V0_SIZE ==
                   V0_END(moq_mpeg2ts_delivery_t, result), "delivery floor");

/* Clear and stamp min(size, full) octets of a sized struct whose first
 * field is its uint32_t struct_size. Returns the octets written: 0 when
 * size cannot hold struct_size (nothing is written). */
static size_t sized_clear(void *p, size_t size, size_t full)
{
    if (!p || size < sizeof(uint32_t)) return 0;
    size_t n = size < full ? size : full;
    memset(p, 0, n);
    uint32_t stamp = (uint32_t)n;
    memcpy(p, &stamp, sizeof(stamp));
    return n;
}

/* Whether a prefix of n octets covers field f of type t whole. */
#define COVERS(n, t, f) ((n) >= V0_END(t, f))

/* Copy a whole record filled by this build to an output of out_size octets
 * and stamp the octets written. out_size is at least the output's floor. */
static void sized_out(void *out, size_t out_size, const void *full,
                      size_t full_size)
{
    size_t n = out_size < full_size ? out_size : full_size;
    memcpy(out, full, n);
    uint32_t stamp = (uint32_t)n;
    memcpy(out, &stamp, sizeof(stamp));
}
#define SW_STATS_V0   V0_END(moq_mpeg2ts_switcher_stats_t, pmt_pids_untracked)
#define PZ_STATS_V0   V0_END(moq_mpeg2ts_packetizer_stats_t, \
                             source_pcr_discontinuities)

/* -- Small helpers ----------------------------------------------------- */

static bool packet_size_ok(uint32_t ps)
{
    return ps == MOQ_MPEG2TS_TS_PACKET_SIZE || ps == MOQ_MPEG2TS_M2TS_PACKET_SIZE;
}

static size_t ts_offset(uint32_t ps)
{
    return ps == MOQ_MPEG2TS_M2TS_PACKET_SIZE ? MOQ_MPEG2TS_PREFIX_SIZE : 0;
}

static uint16_t rd16(const uint8_t *p)
{
    return (uint16_t)(((uint16_t)p[0] << 8) | p[1]);
}

static uint32_t rd32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}

static void wr32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

static uint16_t pkt_pid(const uint8_t *ts)
{
    return (uint16_t)(((ts[1] & 0x1Fu) << 8) | ts[2]);
}

/* Payload carried (adaptation_field_control 01 or 11). */
static bool pkt_has_payload(const uint8_t *ts)
{
    return (ts[3] & 0x10u) != 0;
}

static bool pid_sel_ok(uint16_t pid)
{
    return pid <= MOQ_MPEG2TS_PID_MAX || pid == MOQ_MPEG2TS_PID_NONE ||
           pid == MOQ_MPEG2TS_PID_AUTO;
}

static void bit_set(uint8_t *map, uint16_t pid)
{
    if (pid <= MOQ_MPEG2TS_PID_MAX)
        map[pid >> 3] = (uint8_t)(map[pid >> 3] | (1u << (pid & 7u)));
}

static void bit_clear(uint8_t *map, uint16_t pid)
{
    if (pid <= MOQ_MPEG2TS_PID_MAX)
        map[pid >> 3] = (uint8_t)(map[pid >> 3] & ~(1u << (pid & 7u)));
}

static bool bit_get(const uint8_t *map, uint16_t pid)
{
    return pid <= MOQ_MPEG2TS_PID_MAX && ((map[pid >> 3] >> (pid & 7u)) & 1u);
}

/* (a * b) / d with a 128-bit intermediate, saturating at UINT64_MAX. d > 0. */
static uint64_t muldiv_sat(uint64_t a, uint64_t b, uint64_t d)
{
    uint64_t a_lo = a & 0xFFFFFFFFu, a_hi = a >> 32;
    uint64_t b_lo = b & 0xFFFFFFFFu, b_hi = b >> 32;
    uint64_t ll = a_lo * b_lo;
    uint64_t lh = a_lo * b_hi;
    uint64_t hl = a_hi * b_lo;
    uint64_t hh = a_hi * b_hi;
    uint64_t mid = (ll >> 32) + (lh & 0xFFFFFFFFu) + (hl & 0xFFFFFFFFu);
    uint64_t lo = (ll & 0xFFFFFFFFu) | (mid << 32);
    uint64_t hi = hh + (lh >> 32) + (hl >> 32) + (mid >> 32);
    if (hi >= d) return UINT64_MAX;
    /* Restoring division of hi:lo by d; the quotient fits in 64 bits. */
    uint64_t q = 0, r = hi;
    for (int i = 63; i >= 0; i--) {
        bool carry = (r >> 63) != 0;
        r = (r << 1) | ((lo >> i) & 1u);
        q <<= 1;
        if (carry || r >= d) {
            r -= d;
            q |= 1u;
        }
    }
    return q;
}

/* -- CRC_32 ------------------------------------------------------------- */

/* CRC_32/MPEG-2 table for polynomial 0x04C11DB7, most significant bit first.
 * Constant data: concurrent callers share nothing writable. */
static const uint32_t CRC_TABLE[256] = {
    0x00000000u, 0x04C11DB7u, 0x09823B6Eu, 0x0D4326D9u,
    0x130476DCu, 0x17C56B6Bu, 0x1A864DB2u, 0x1E475005u,
    0x2608EDB8u, 0x22C9F00Fu, 0x2F8AD6D6u, 0x2B4BCB61u,
    0x350C9B64u, 0x31CD86D3u, 0x3C8EA00Au, 0x384FBDBDu,
    0x4C11DB70u, 0x48D0C6C7u, 0x4593E01Eu, 0x4152FDA9u,
    0x5F15ADACu, 0x5BD4B01Bu, 0x569796C2u, 0x52568B75u,
    0x6A1936C8u, 0x6ED82B7Fu, 0x639B0DA6u, 0x675A1011u,
    0x791D4014u, 0x7DDC5DA3u, 0x709F7B7Au, 0x745E66CDu,
    0x9823B6E0u, 0x9CE2AB57u, 0x91A18D8Eu, 0x95609039u,
    0x8B27C03Cu, 0x8FE6DD8Bu, 0x82A5FB52u, 0x8664E6E5u,
    0xBE2B5B58u, 0xBAEA46EFu, 0xB7A96036u, 0xB3687D81u,
    0xAD2F2D84u, 0xA9EE3033u, 0xA4AD16EAu, 0xA06C0B5Du,
    0xD4326D90u, 0xD0F37027u, 0xDDB056FEu, 0xD9714B49u,
    0xC7361B4Cu, 0xC3F706FBu, 0xCEB42022u, 0xCA753D95u,
    0xF23A8028u, 0xF6FB9D9Fu, 0xFBB8BB46u, 0xFF79A6F1u,
    0xE13EF6F4u, 0xE5FFEB43u, 0xE8BCCD9Au, 0xEC7DD02Du,
    0x34867077u, 0x30476DC0u, 0x3D044B19u, 0x39C556AEu,
    0x278206ABu, 0x23431B1Cu, 0x2E003DC5u, 0x2AC12072u,
    0x128E9DCFu, 0x164F8078u, 0x1B0CA6A1u, 0x1FCDBB16u,
    0x018AEB13u, 0x054BF6A4u, 0x0808D07Du, 0x0CC9CDCAu,
    0x7897AB07u, 0x7C56B6B0u, 0x71159069u, 0x75D48DDEu,
    0x6B93DDDBu, 0x6F52C06Cu, 0x6211E6B5u, 0x66D0FB02u,
    0x5E9F46BFu, 0x5A5E5B08u, 0x571D7DD1u, 0x53DC6066u,
    0x4D9B3063u, 0x495A2DD4u, 0x44190B0Du, 0x40D816BAu,
    0xACA5C697u, 0xA864DB20u, 0xA527FDF9u, 0xA1E6E04Eu,
    0xBFA1B04Bu, 0xBB60ADFCu, 0xB6238B25u, 0xB2E29692u,
    0x8AAD2B2Fu, 0x8E6C3698u, 0x832F1041u, 0x87EE0DF6u,
    0x99A95DF3u, 0x9D684044u, 0x902B669Du, 0x94EA7B2Au,
    0xE0B41DE7u, 0xE4750050u, 0xE9362689u, 0xEDF73B3Eu,
    0xF3B06B3Bu, 0xF771768Cu, 0xFA325055u, 0xFEF34DE2u,
    0xC6BCF05Fu, 0xC27DEDE8u, 0xCF3ECB31u, 0xCBFFD686u,
    0xD5B88683u, 0xD1799B34u, 0xDC3ABDEDu, 0xD8FBA05Au,
    0x690CE0EEu, 0x6DCDFD59u, 0x608EDB80u, 0x644FC637u,
    0x7A089632u, 0x7EC98B85u, 0x738AAD5Cu, 0x774BB0EBu,
    0x4F040D56u, 0x4BC510E1u, 0x46863638u, 0x42472B8Fu,
    0x5C007B8Au, 0x58C1663Du, 0x558240E4u, 0x51435D53u,
    0x251D3B9Eu, 0x21DC2629u, 0x2C9F00F0u, 0x285E1D47u,
    0x36194D42u, 0x32D850F5u, 0x3F9B762Cu, 0x3B5A6B9Bu,
    0x0315D626u, 0x07D4CB91u, 0x0A97ED48u, 0x0E56F0FFu,
    0x1011A0FAu, 0x14D0BD4Du, 0x19939B94u, 0x1D528623u,
    0xF12F560Eu, 0xF5EE4BB9u, 0xF8AD6D60u, 0xFC6C70D7u,
    0xE22B20D2u, 0xE6EA3D65u, 0xEBA91BBCu, 0xEF68060Bu,
    0xD727BBB6u, 0xD3E6A601u, 0xDEA580D8u, 0xDA649D6Fu,
    0xC423CD6Au, 0xC0E2D0DDu, 0xCDA1F604u, 0xC960EBB3u,
    0xBD3E8D7Eu, 0xB9FF90C9u, 0xB4BCB610u, 0xB07DABA7u,
    0xAE3AFBA2u, 0xAAFBE615u, 0xA7B8C0CCu, 0xA379DD7Bu,
    0x9B3660C6u, 0x9FF77D71u, 0x92B45BA8u, 0x9675461Fu,
    0x8832161Au, 0x8CF30BADu, 0x81B02D74u, 0x857130C3u,
    0x5D8A9099u, 0x594B8D2Eu, 0x5408ABF7u, 0x50C9B640u,
    0x4E8EE645u, 0x4A4FFBF2u, 0x470CDD2Bu, 0x43CDC09Cu,
    0x7B827D21u, 0x7F436096u, 0x7200464Fu, 0x76C15BF8u,
    0x68860BFDu, 0x6C47164Au, 0x61043093u, 0x65C52D24u,
    0x119B4BE9u, 0x155A565Eu, 0x18197087u, 0x1CD86D30u,
    0x029F3D35u, 0x065E2082u, 0x0B1D065Bu, 0x0FDC1BECu,
    0x3793A651u, 0x3352BBE6u, 0x3E119D3Fu, 0x3AD08088u,
    0x2497D08Du, 0x2056CD3Au, 0x2D15EBE3u, 0x29D4F654u,
    0xC5A92679u, 0xC1683BCEu, 0xCC2B1D17u, 0xC8EA00A0u,
    0xD6AD50A5u, 0xD26C4D12u, 0xDF2F6BCBu, 0xDBEE767Cu,
    0xE3A1CBC1u, 0xE760D676u, 0xEA23F0AFu, 0xEEE2ED18u,
    0xF0A5BD1Du, 0xF464A0AAu, 0xF9278673u, 0xFDE69BC4u,
    0x89B8FD09u, 0x8D79E0BEu, 0x803AC667u, 0x84FBDBD0u,
    0x9ABC8BD5u, 0x9E7D9662u, 0x933EB0BBu, 0x97FFAD0Cu,
    0xAFB010B1u, 0xAB710D06u, 0xA6322BDFu, 0xA2F33668u,
    0xBCB4666Du, 0xB8757BDAu, 0xB5365D03u, 0xB1F740B4u,
};

static uint32_t crc_step(uint32_t c, uint8_t b)
{
    return (c << 8) ^ CRC_TABLE[((c >> 24) ^ b) & 0xFFu];
}

uint32_t moq_mpeg2ts_crc32(const uint8_t *data, size_t len)
{
    uint32_t c = 0xFFFFFFFFu;
    if (!data) return c;
    for (size_t i = 0; i < len; i++) c = crc_step(c, data[i]);
    return c;
}

/* -- Packet parse ------------------------------------------------------- */

void moq_mpeg2ts_packet_init(moq_mpeg2ts_packet_t *pkt)
{
    if (!pkt) return;
    memset(pkt, 0, sizeof(*pkt));
}

moq_result_t moq_mpeg2ts_parse_packet(moq_bytes_t src, uint32_t packet_size,
                                      moq_mpeg2ts_packet_t *out)
{
    if (!out || !src.data || !packet_size_ok(packet_size) ||
        src.len < packet_size)
        return MOQ_ERR_INVAL;
    moq_mpeg2ts_packet_init(out);

    const uint8_t *p = src.data;
    if (packet_size == MOQ_MPEG2TS_M2TS_PACKET_SIZE) {
        out->has_prefix = true;
        out->prefix = rd32(p);
        p += MOQ_MPEG2TS_PREFIX_SIZE;
    }
    if (p[0] != MOQ_MPEG2TS_SYNC_BYTE) return MOQ_ERR_PROTO;

    out->transport_error = (p[1] & 0x80u) != 0;
    out->payload_unit_start = (p[1] & 0x40u) != 0;
    out->transport_priority = (p[1] & 0x20u) != 0;
    out->pid = pkt_pid(p);
    out->scrambling = (uint8_t)(p[3] >> 6);
    out->adaptation_field_control = (uint8_t)((p[3] >> 4) & 3u);
    out->continuity_counter = (uint8_t)(p[3] & 0x0Fu);

    uint8_t afc = out->adaptation_field_control;
    if (afc == 0) return MOQ_ERR_PROTO;          /* reserved */

    size_t off = 4;
    if (afc & 2u) {
        size_t af_len = p[4];
        /* 2.4.3.5: 183 with no payload, 0..182 when a payload follows. */
        if (afc == 2 && af_len != 183) return MOQ_ERR_PROTO;
        if (afc == 3 && af_len > 182) return MOQ_ERR_PROTO;
        out->has_adaptation_field = true;
        out->adaptation_field_length = (uint8_t)af_len;
        if (af_len > 0) {
            uint8_t flags = p[5];
            out->discontinuity = (flags & 0x80u) != 0;
            out->random_access = (flags & 0x40u) != 0;
            out->es_priority = (flags & 0x20u) != 0;
            /* Optional fields in order (Table 2-6): PCR 6, OPCR 6,
             * splice_countdown 1, transport_private_data_length + data,
             * adaptation_field_extension_length + extension. `used` counts
             * the flags octet and the fields so far; p[5 + used] is the
             * next octet. */
            size_t used = 1;
            if (flags & 0x10u) used += 6;
            if (flags & 0x08u) used += 6;
            if (flags & 0x04u) used += 1;
            if (used > af_len) return MOQ_ERR_PROTO;
            if (flags & 0x02u) {
                if (used + 1 > af_len) return MOQ_ERR_PROTO;
                used += 1u + p[5 + used];
                if (used > af_len) return MOQ_ERR_PROTO;
            }
            if (flags & 0x01u) {
                if (used + 1 > af_len) return MOQ_ERR_PROTO;
                used += 1u + p[5 + used];
                if (used > af_len) return MOQ_ERR_PROTO;
            }
            if (flags & 0x10u) {
                const uint8_t *c = p + 6;
                uint16_t ext = (uint16_t)(((c[4] & 1u) << 8) | c[5]);
                if (ext >= 300) return MOQ_ERR_PROTO;   /* 2.4.3.5 */
                out->has_pcr = true;
                out->pcr_base = ((uint64_t)c[0] << 25) | ((uint64_t)c[1] << 17) |
                                ((uint64_t)c[2] << 9) | ((uint64_t)c[3] << 1) |
                                ((uint64_t)c[4] >> 7);
                out->pcr_ext = ext;
            }
        }
        off = 5 + af_len;
    }
    if (afc & 1u) {
        out->payload.data = p + off;
        out->payload.len = MOQ_MPEG2TS_TS_PACKET_SIZE - off;
    }
    return MOQ_OK;
}

/* -- PES ---------------------------------------------------------------- */

/* stream_ids without the optional PES header (2.4.3.7, Table 2-21). */
static bool pes_has_optional_header(uint8_t sid)
{
    switch (sid) {
    case 0xBC: case 0xBE: case 0xBF: case 0xF0: case 0xF1: case 0xFF:
    case 0xF2: case 0xF8:
        return false;
    default:
        return true;
    }
}

static bool read_ts(const uint8_t *p, uint8_t want_prefix, uint64_t *out)
{
    if ((p[0] >> 4) != want_prefix) return false;
    if (!(p[0] & 1u) || !(p[2] & 1u) || !(p[4] & 1u)) return false;
    *out = ((uint64_t)((p[0] >> 1) & 7u) << 30) |
           ((uint64_t)p[1] << 22) | ((uint64_t)(p[2] >> 1) << 15) |
           ((uint64_t)p[3] << 7) | (uint64_t)(p[4] >> 1);
    return true;
}

moq_result_t moq_mpeg2ts_parse_pes_header(moq_bytes_t pes,
                                          moq_mpeg2ts_pes_header_t *out)
{
    if (!out || (!pes.data && pes.len)) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    if (pes.len < 6) return MOQ_ERR_PROTO;
    const uint8_t *p = pes.data;
    if (p[0] != 0 || p[1] != 0 || p[2] != 1) return MOQ_ERR_PROTO;
    out->stream_id = p[3];
    if (!pes_has_optional_header(p[3])) return MOQ_OK;
    if (pes.len < 9) return MOQ_ERR_PROTO;
    size_t pes_len = rd16(p + 4);
    if ((p[6] & 0xC0u) != 0x80u) return MOQ_ERR_PROTO;   /* '10' marker */
    uint8_t flags = (uint8_t)(p[7] >> 6);
    size_t hdr_len = p[8];
    if (9 + hdr_len > pes.len) return MOQ_ERR_PROTO;
    /* PES_packet_length counts the octets after it; 0 is the unbounded
     * video form. The optional header must lie inside the declared packet. */
    if (pes_len != 0 && 3 + hdr_len > pes_len) return MOQ_ERR_PROTO;
    if (flags == 1) return MOQ_ERR_PROTO;                 /* forbidden */
    if (flags == 2) {
        if (hdr_len < 5 || !read_ts(p + 9, 2, &out->pts)) return MOQ_ERR_PROTO;
        out->has_pts = true;
    } else if (flags == 3) {
        if (hdr_len < 10 || !read_ts(p + 9, 3, &out->pts) ||
            !read_ts(p + 14, 1, &out->dts))
            return MOQ_ERR_PROTO;
        out->has_pts = true;
        out->has_dts = true;
    }
    return MOQ_OK;
}

/* A PES packet starts in this packet's payload. */
static bool pes_start(const moq_mpeg2ts_packet_t *p)
{
    const uint8_t *d = p->payload.data;
    return p->payload_unit_start && d && p->payload.len >= 4 &&
           d[0] == 0 && d[1] == 0 && d[2] == 1;
}

/* That PES carries a video stream_id (Table 2-22: 1110 xxxx). */
static bool pes_is_video(const moq_mpeg2ts_packet_t *p)
{
    return pes_start(p) && (p->payload.data[3] & 0xF0u) == 0xE0u;
}

/* -- Section reassembly ------------------------------------------------- */

static void sec_reset(moq_mpeg2ts_section_state_t *s)
{
    s->_len = 0;
    s->_need = 0;
    s->_active = false;
}

static void sec_forget(moq_mpeg2ts_section_state_t *s)
{
    memset(s, 0, sizeof(*s));
}

/* Load one packet; its sections are then read with sec_next. */
static moq_result_t sec_push(moq_mpeg2ts_section_state_t *s,
                             const moq_mpeg2ts_packet_t *pkt)
{
    s->_in = NULL;
    s->_in_len = 0;
    s->_pos = 0;
    s->_cont_end = 0;
    s->_pusi = false;
    s->_started = false;
    const uint8_t *d = pkt->payload.data;
    size_t n = pkt->payload.len;
    /* A packet without payload keeps its counter and carries no section. */
    if (!d || n == 0) return MOQ_OK;

    if (s->_have_last && s->_last_pid == pkt->pid &&
        s->_last_cc == pkt->continuity_counter) {
        /* A duplicate repeats the packet's bytes (2.4.3.3): its
         * payload_unit_start_indicator as well as its payload. Equal
         * checksums prove nothing, since PSI payloads carry their own
         * CRC_32, and the same payload read with another
         * payload_unit_start_indicator holds other sections. */
        if (s->_last_pusi == pkt->payload_unit_start &&
            s->_last_len == n && memcmp(s->_last_payload, d, n) == 0)
            return MOQ_OK;
        sec_reset(s);                     /* same counter, other payload */
    } else if (s->_active &&
               (s->_pid != pkt->pid ||
                pkt->continuity_counter != ((s->_last_cc + 1u) & 0x0Fu))) {
        sec_reset(s);                     /* lost or foreign packet */
    }
    s->_have_last = true;
    s->_last_pid = pkt->pid;
    s->_last_cc = pkt->continuity_counter;
    s->_last_len = (uint8_t)n;
    s->_last_pusi = pkt->payload_unit_start;
    memcpy(s->_last_payload, d, n);

    if (pkt->payload_unit_start) {
        size_t ptr = d[0];
        if (1 + ptr > n) {
            sec_reset(s);
            return MOQ_ERR_PROTO;
        }
        s->_in = d;
        s->_in_len = n;
        s->_pusi = true;
        s->_pos = 1;
        s->_cont_end = 1 + ptr;
        return MOQ_OK;
    }
    if (!s->_active) return MOQ_OK;
    s->_in = d;
    s->_in_len = n;
    s->_pos = 0;
    s->_cont_end = n;
    return MOQ_OK;
}

/* Collect up to n octets of the section in progress. Returns 1 when it is
 * complete, 0 when more is needed, -1 when it must be abandoned. */
static int sec_collect(moq_mpeg2ts_section_state_t *s, uint8_t *st, size_t cap,
                       const uint8_t *d, size_t n, size_t *used)
{
    size_t u = 0;
    if (s->_need == 0) {
        size_t take = 3 - s->_len;
        if (take > n) take = n;
        memcpy(st + s->_len, d, take);
        s->_len += take;
        u += take;
        if (s->_len < 3) {
            *used = u;
            return 0;
        }
        size_t sl = ((size_t)(st[1] & 0x0Fu) << 8) | st[2];
        if (sl > MOQ_MPEG2TS_SECTION_MAX - 3 || 3 + sl > cap) {
            *used = u;
            return -1;
        }
        s->_need = 3 + sl;
    }
    size_t take = s->_need - s->_len;
    if (take > n - u) take = n - u;
    memcpy(st + s->_len, d + u, take);
    s->_len += take;
    u += take;
    *used = u;
    return s->_len == s->_need ? 1 : 0;
}

/* The next complete section of the loaded packet, if any. */
static bool sec_next(moq_mpeg2ts_section_state_t *s, uint8_t *st, size_t cap,
                     moq_bytes_t *out)
{
    if (!s->_in) return false;

    /* Octets that continue the section in progress: the whole payload of a
     * continuation packet, or those before the pointer of a PUSI packet. */
    if (!s->_started) {
        if (s->_active) {
            size_t used = 0;
            int r = 0;
            if (s->_pos < s->_cont_end)
                r = sec_collect(s, st, cap, s->_in + s->_pos,
                                s->_cont_end - s->_pos, &used);
            s->_pos = s->_cont_end;
            s->_started = true;
            if (r > 0) {
                s->_active = false;
                out->data = st;
                out->len = s->_need;
                if (!s->_pusi) s->_in = NULL;   /* the rest is stuffing */
                return true;
            }
            /* A PUSI packet whose pointer cuts the section short, or a
             * section that cannot be collected: abandon it. */
            if (r < 0 || s->_pusi) sec_reset(s);
        }
        s->_started = true;
        s->_pos = s->_cont_end;
    }
    if (!s->_pusi) {
        s->_in = NULL;
        return false;
    }

    /* New sections, back to back after the pointer, until stuffing. */
    while (s->_pos < s->_in_len) {
        const uint8_t *d = s->_in + s->_pos;
        size_t left = s->_in_len - s->_pos;
        if (d[0] == 0xFF) break;
        if (left >= 3) {
            size_t sl = ((size_t)(d[1] & 0x0Fu) << 8) | d[2];
            if (sl > MOQ_MPEG2TS_SECTION_MAX - 3) break;  /* cannot resync */
            if (3 + sl <= left) {
                out->data = d;
                out->len = 3 + sl;
                s->_pos += 3 + sl;
                return true;
            }
        }
        /* Starts here, continues in the next packet. */
        sec_reset(s);
        s->_active = true;
        s->_pid = s->_last_pid;
        size_t used = 0;
        if (sec_collect(s, st, cap, d, left, &used) < 0) sec_reset(s);
        break;
    }
    s->_in = NULL;
    return false;
}

moq_result_t moq_mpeg2ts_section_buf_init(moq_mpeg2ts_section_buf_t *sb,
                                          size_t sb_size, uint8_t *storage,
                                          size_t cap)
{
    if (!sb || sb_size < sizeof(*sb) || !storage || cap < 3)
        return MOQ_ERR_INVAL;
    memset(sb, 0, sizeof(*sb));
    sb->struct_size = sizeof(*sb);
    sb->_storage = storage;
    sb->_cap = cap;
    return MOQ_OK;
}

void moq_mpeg2ts_section_buf_reset(moq_mpeg2ts_section_buf_t *sb)
{
    if (!sb) return;
    sec_forget(&sb->_s);
}

moq_result_t moq_mpeg2ts_section_buf_push(moq_mpeg2ts_section_buf_t *sb,
                                          const moq_mpeg2ts_packet_t *pkt)
{
    if (!sb || !pkt || sb->struct_size != sizeof(*sb) || !sb->_storage)
        return MOQ_ERR_INVAL;
    return sec_push(&sb->_s, pkt);
}

moq_result_t moq_mpeg2ts_section_buf_next(moq_mpeg2ts_section_buf_t *sb,
                                          moq_bytes_t *section)
{
    if (!sb || !section || sb->struct_size != sizeof(*sb) || !sb->_storage)
        return MOQ_ERR_INVAL;
    section->data = NULL;
    section->len = 0;
    return sec_next(&sb->_s, sb->_storage, sb->_cap, section) ? MOQ_OK
                                                               : MOQ_DONE;
}

/* -- PSI tables ---------------------------------------------------------- */

/* Common long-form section checks. On success body and body_len cover the
 * octets between the eight-octet header and the CRC_32. */
static moq_result_t section_check(moq_bytes_t s, uint8_t table_id,
                                  const uint8_t **body, size_t *body_len)
{
    if (!s.data) return MOQ_ERR_INVAL;
    if (s.len < 12) return MOQ_ERR_PROTO;
    const uint8_t *p = s.data;
    if (p[0] != table_id) return MOQ_ERR_PROTO;
    if (!(p[1] & 0x80u)) return MOQ_ERR_PROTO;            /* syntax indicator */
    if (p[1] & 0x40u) return MOQ_ERR_PROTO;               /* '0' bit */
    size_t sl = ((size_t)(p[1] & 0x0Fu) << 8) | p[2];
    if (sl > MOQ_MPEG2TS_SECTION_MAX - 3 || sl < 9) return MOQ_ERR_PROTO;
    if (3 + sl != s.len) return MOQ_ERR_PROTO;
    if (p[6] > p[7]) return MOQ_ERR_PROTO;   /* section_number > last */
    if (moq_mpeg2ts_crc32(p, s.len) != 0) return MOQ_ERR_PROTO;
    *body = p + 8;
    *body_len = s.len - 8 - 4;
    return MOQ_OK;
}

/* Every descriptor (tag, length, data) lies inside the loop. */
static bool desc_loop_ok(const uint8_t *d, size_t n)
{
    size_t i = 0;
    while (i < n) {
        if (n - i < 2) return false;
        size_t len = d[i + 1];
        if (len > n - i - 2) return false;
        i += 2 + len;
    }
    return true;
}

void moq_mpeg2ts_pat_init(moq_mpeg2ts_pat_t *pat,
                          moq_mpeg2ts_program_t *programs, size_t cap)
{
    if (!pat) return;
    memset(pat, 0, sizeof(*pat));
    pat->programs = programs;
    pat->program_cap = programs ? cap : 0;
}

moq_result_t moq_mpeg2ts_parse_pat(moq_bytes_t section, moq_mpeg2ts_pat_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    const uint8_t *body;
    size_t blen;
    moq_result_t rc = section_check(section, 0x00, &body, &blen);
    if (rc != MOQ_OK) return rc;
    if (blen % 4 != 0) return MOQ_ERR_PROTO;

    const uint8_t *p = section.data;
    out->transport_stream_id = rd16(p + 3);
    out->version = (uint8_t)((p[5] >> 1) & 0x1Fu);
    out->current_next = (p[5] & 1u) != 0;
    out->section_number = p[6];
    out->last_section_number = p[7];
    out->has_network_pid = false;
    out->network_pid = 0;

    size_t count = 0;
    for (size_t i = 0; i < blen; i += 4) {
        uint16_t num = rd16(body + i);
        uint16_t pid = (uint16_t)(rd16(body + i + 2) & 0x1FFFu);
        if (num == 0) {
            out->has_network_pid = true;
            out->network_pid = pid;
            continue;
        }
        if (count < out->program_cap) {
            out->programs[count].program_number = num;
            out->programs[count].pmt_pid = pid;
        }
        count++;
    }
    out->program_count = count;
    return count > out->program_cap ? MOQ_ERR_BUFFER : MOQ_OK;
}

void moq_mpeg2ts_pmt_init(moq_mpeg2ts_pmt_t *pmt,
                          moq_mpeg2ts_stream_t *streams, size_t cap)
{
    if (!pmt) return;
    memset(pmt, 0, sizeof(*pmt));
    pmt->streams = streams;
    pmt->stream_cap = streams ? cap : 0;
}

moq_result_t moq_mpeg2ts_parse_pmt(moq_bytes_t section, moq_mpeg2ts_pmt_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    const uint8_t *body;
    size_t blen;
    moq_result_t rc = section_check(section, 0x02, &body, &blen);
    if (rc != MOQ_OK) return rc;
    if (blen < 4) return MOQ_ERR_PROTO;
    const uint8_t *p = section.data;
    if (p[6] != 0 || p[7] != 0) return MOQ_ERR_PROTO;     /* 2.4.4.9 */

    size_t pil = rd16(body + 2) & 0x0FFFu;
    if (4 + pil > blen || !desc_loop_ok(body + 4, pil)) return MOQ_ERR_PROTO;
    /* Validate every entry before writing any of the caller's table. */
    size_t count = 0;
    for (size_t i = 4 + pil; i < blen;) {
        if (blen - i < 5) return MOQ_ERR_PROTO;
        size_t eil = rd16(body + i + 3) & 0x0FFFu;
        if (eil > blen - i - 5 || !desc_loop_ok(body + i + 5, eil))
            return MOQ_ERR_PROTO;
        count++;
        i += 5 + eil;
    }

    out->program_number = rd16(p + 3);
    out->version = (uint8_t)((p[5] >> 1) & 0x1Fu);
    out->current_next = (p[5] & 1u) != 0;
    out->pcr_pid = (uint16_t)(rd16(body) & 0x1FFFu);
    out->program_descriptors.data = body + 4;
    out->program_descriptors.len = pil;
    size_t k = 0;
    for (size_t i = 4 + pil; i < blen && k < out->stream_cap; k++) {
        size_t eil = rd16(body + i + 3) & 0x0FFFu;
        out->streams[k].stream_type = body[i];
        out->streams[k].pid = (uint16_t)(rd16(body + i + 1) & 0x1FFFu);
        out->streams[k].descriptors.data = body + i + 5;
        out->streams[k].descriptors.len = eil;
        i += 5 + eil;
    }
    out->stream_count = count;
    return count > out->stream_cap ? MOQ_ERR_BUFFER : MOQ_OK;
}

bool moq_mpeg2ts_stream_type_is_video(uint8_t t)
{
    switch (t) {
    case 0x01: /* ISO/IEC 11172-2 video */
    case 0x02: /* ITU-T H.262 | ISO/IEC 13818-2 video */
    case 0x10: /* ISO/IEC 14496-2 visual */
    case 0x1B: /* ITU-T H.264 | ISO/IEC 14496-10 (AVC) */
    case 0x1F: /* AVC SVC sub-bitstream */
    case 0x20: /* AVC MVC sub-bitstream */
    case 0x21: /* ITU-T T.800 | ISO/IEC 15444-1 (JPEG 2000) video */
    case 0x24: /* ITU-T H.265 | ISO/IEC 23008-2 (HEVC) */
    case 0x25: /* HEVC temporal video subset */
    case 0x32: /* ISO/IEC 21122-2 (JPEG XS) video */
    case 0x33: /* ITU-T H.266 | ISO/IEC 23090-3 (VVC) */
    case 0x34: /* VVC temporal sub-bitstream */
        return true;
    default:
        return false;
    }
}

/* First video stream of a PMT section already checked by parse_pmt. */
static uint16_t pmt_first_video(moq_bytes_t sec)
{
    const uint8_t *body = sec.data + 8;
    size_t blen = sec.len - 12;
    size_t pil = rd16(body + 2) & 0x0FFFu;
    for (size_t i = 4 + pil; i + 5 <= blen;) {
        size_t eil = rd16(body + i + 3) & 0x0FFFu;
        if (moq_mpeg2ts_stream_type_is_video(body[i]))
            return (uint16_t)(rd16(body + i + 1) & 0x1FFFu);
        i += 5 + eil;
    }
    return MOQ_MPEG2TS_PID_NONE;
}

moq_result_t moq_mpeg2ts_ca_pids(moq_bytes_t d, uint16_t *pids, size_t cap,
                                 size_t *count)
{
    if (!count || (!d.data && d.len) || (!pids && cap)) return MOQ_ERR_INVAL;
    *count = 0;
    size_t i = 0, n = 0;
    while (i < d.len) {
        if (d.len - i < 2) return MOQ_ERR_PROTO;
        uint8_t tag = d.data[i];
        size_t len = d.data[i + 1];
        if (len > d.len - i - 2) return MOQ_ERR_PROTO;
        if (tag == 0x09) {                      /* CA_descriptor, 2.6.16 */
            if (len < 4) return MOQ_ERR_PROTO;
            if (n < cap)
                pids[n] = (uint16_t)(rd16(d.data + i + 4) & 0x1FFFu);
            n++;
        }
        i += 2 + len;
    }
    *count = n;
    return n > cap ? MOQ_ERR_BUFFER : MOQ_OK;
}

/* Set the CA_PID of every well-formed CA_descriptor of a loop in `map`,
 * without any limit on their number. Stops at a malformed descriptor. */
static void map_ca_pids(uint8_t *map, const uint8_t *d, size_t n)
{
    size_t i = 0;
    while (i + 2 <= n) {
        size_t len = d[i + 1];
        if (len > n - i - 2) return;
        if (d[i] == 0x09 && len >= 4)
            bit_set(map, (uint16_t)(rd16(d + i + 4) & 0x1FFFu));
        i += 2 + len;
    }
}

static void section_header(uint8_t *s, uint8_t table_id, uint16_t ext,
                           uint8_t version, size_t total_len)
{
    size_t sl = total_len - 3;
    s[0] = table_id;
    s[1] = (uint8_t)(0xB0u | ((sl >> 8) & 0x0Fu));
    s[2] = (uint8_t)sl;
    s[3] = (uint8_t)(ext >> 8);
    s[4] = (uint8_t)ext;
    s[5] = (uint8_t)(0xC1u | ((version & 0x1Fu) << 1));
    s[6] = 0;
    s[7] = 0;
}

moq_result_t moq_mpeg2ts_build_pat(uint16_t tsid, uint8_t version,
                                   const moq_mpeg2ts_program_t *programs,
                                   size_t count, uint8_t *out, size_t cap,
                                   size_t *out_len)
{
    if (!out_len || (!programs && count) || version > 31) return MOQ_ERR_INVAL;
    if (count > (MOQ_MPEG2TS_SECTION_MAX - 12) / 4) return MOQ_ERR_INVAL;
    for (size_t i = 0; i < count; i++)
        if (programs[i].program_number == 0 ||
            programs[i].pmt_pid > MOQ_MPEG2TS_PID_MAX)
            return MOQ_ERR_INVAL;
    size_t total = 8 + 4 * count + 4;
    *out_len = total;
    if (!out || cap < total) return MOQ_ERR_BUFFER;
    section_header(out, 0x00, tsid, version, total);
    for (size_t i = 0; i < count; i++) {
        uint8_t *e = out + 8 + 4 * i;
        e[0] = (uint8_t)(programs[i].program_number >> 8);
        e[1] = (uint8_t)programs[i].program_number;
        e[2] = (uint8_t)(0xE0u | (programs[i].pmt_pid >> 8));
        e[3] = (uint8_t)programs[i].pmt_pid;
    }
    wr32(out + total - 4, moq_mpeg2ts_crc32(out, total - 4));
    return MOQ_OK;
}

static bool pid_listed(const uint16_t *pids, size_t n, uint16_t pid)
{
    for (size_t i = 0; i < n; i++)
        if (pids[i] == pid) return true;
    return false;
}

moq_result_t moq_mpeg2ts_pmt_subset(moq_bytes_t pmt_section,
                                    const uint16_t *keep, size_t keep_count,
                                    uint8_t *out, size_t cap, size_t *out_len)
{
    if (!out_len || (!keep && keep_count)) return MOQ_ERR_INVAL;
    *out_len = 0;
    moq_mpeg2ts_pmt_t pmt;
    moq_mpeg2ts_pmt_init(&pmt, NULL, 0);
    moq_result_t rc = moq_mpeg2ts_parse_pmt(pmt_section, &pmt);
    if (rc != MOQ_OK && rc != MOQ_ERR_BUFFER) return rc;

    /* Size pass, then write pass over the same entry walk. */
    const uint8_t *body = pmt_section.data + 8;
    size_t blen = pmt_section.len - 12;
    size_t pil = pmt.program_descriptors.len;
    size_t total = 8 + 4 + pil + 4;
    for (size_t i = 4 + pil; i < blen;) {
        uint16_t pid = (uint16_t)(rd16(body + i + 1) & 0x1FFFu);
        size_t eil = rd16(body + i + 3) & 0x0FFFu;
        if (pid_listed(keep, keep_count, pid)) total += 5 + eil;
        i += 5 + eil;
    }
    *out_len = total;
    if (!out || cap < total) return MOQ_ERR_BUFFER;

    uint8_t version = (uint8_t)((pmt.version + 1u) & 0x1Fu);
    section_header(out, 0x02, pmt.program_number, version, total);
    out[5] = (uint8_t)((out[5] & ~1u) | (pmt.current_next ? 1u : 0u));
    memcpy(out + 8, body, 4 + pil);
    size_t o = 12 + pil;
    for (size_t i = 4 + pil; i < blen;) {
        uint16_t pid = (uint16_t)(rd16(body + i + 1) & 0x1FFFu);
        size_t eil = rd16(body + i + 3) & 0x0FFFu;
        if (pid_listed(keep, keep_count, pid)) {
            memcpy(out + o, body + i, 5 + eil);
            o += 5 + eil;
        }
        i += 5 + eil;
    }
    wr32(out + o, moq_mpeg2ts_crc32(out, o));
    return MOQ_OK;
}

moq_result_t moq_mpeg2ts_packetize_section(uint16_t pid, uint8_t *cc,
                                           moq_bytes_t section,
                                           uint32_t packet_size,
                                           uint8_t *out, size_t cap,
                                           size_t *out_len)
{
    if (!cc || !out_len || !section.data || section.len == 0 ||
        !packet_size_ok(packet_size) || pid > MOQ_MPEG2TS_PID_MAX)
        return MOQ_ERR_INVAL;
    /* 183 octets in the first packet (after pointer_field), 184 after. */
    size_t packets = 1;
    if (section.len > 183) packets += (section.len - 183 + 183) / 184;
    size_t total = packets * packet_size;
    *out_len = total;
    if (!out || cap < total) return MOQ_ERR_BUFFER;

    size_t off = ts_offset(packet_size);
    size_t done = 0;
    for (size_t k = 0; k < packets; k++) {
        uint8_t *pk = out + k * packet_size;
        if (off) memset(pk, 0, off);
        uint8_t *t = pk + off;
        memset(t, 0xFF, MOQ_MPEG2TS_TS_PACKET_SIZE);
        t[0] = MOQ_MPEG2TS_SYNC_BYTE;
        t[1] = (uint8_t)((k == 0 ? 0x40u : 0u) | (pid >> 8));
        t[2] = (uint8_t)pid;
        t[3] = (uint8_t)(0x10u | (*cc & 0x0Fu));
        *cc = (uint8_t)((*cc + 1u) & 0x0Fu);
        size_t room = 184;
        uint8_t *d = t + 4;
        if (k == 0) { d[0] = 0; d++; room--; }
        size_t take = section.len - done;
        if (take > room) take = room;
        memcpy(d, section.data + done, take);
        done += take;
    }
    return MOQ_OK;
}

/* -- Program tracking shared by the receiver and the packetizer -------- */

/* What a stream's PAT and PMT say about the program to follow. */
typedef struct psi_view {
    uint16_t want_program;           /* 0: the first program of the PAT */
    uint16_t *pmt_pid;
    bool     *have_pmt;
    uint16_t *video_pid;
    uint16_t *pcr_pid;               /* may be NULL */
    /* With want_program 0: the section the reference program came from. */
    bool     *sel_have;
    uint8_t  *sel_version;
    uint8_t  *sel_section;
    /* The program whose PMT applies: want_program, or the one selected. */
    uint16_t *sel_program;
    moq_mpeg2ts_section_state_t *pat_s;
    moq_mpeg2ts_section_state_t *pmt_s;
    uint8_t  *pat_store;
    uint8_t  *pmt_store;
} psi_view_t;

/* Feed one packet of the stream to the PAT/PMT tracking. */
static void psi_feed(const psi_view_t *v, const moq_mpeg2ts_packet_t *p)
{
    moq_bytes_t sec;
    if (p->pid == MOQ_MPEG2TS_PID_PAT) {
        if (sec_push(v->pat_s, p) != MOQ_OK) return;
        while (sec_next(v->pat_s, v->pat_store, MOQ_MPEG2TS_SECTION_MAX, &sec)) {
            moq_mpeg2ts_pat_t pat;
            moq_mpeg2ts_pat_init(&pat, NULL, 0);
            moq_result_t rc = moq_mpeg2ts_parse_pat(sec, &pat);
            if ((rc != MOQ_OK && rc != MOQ_ERR_BUFFER) || !pat.current_next)
                continue;
            /* With no program named, the reference is the first program of
             * the PAT (Unmodified Multiplex): the first entry of the
             * lowest-numbered section of the current version that lists
             * one. A later section, or a repetition, does not move it. */
            if (v->want_program == 0 && *v->sel_have &&
                pat.version == *v->sel_version &&
                pat.section_number > *v->sel_section)
                continue;
            const uint8_t *body = sec.data + 8;
            for (size_t k = 0; k + 4 <= sec.len - 12; k += 4) {
                uint16_t num = rd16(body + k);
                if (num == 0) continue;
                if (v->want_program != 0 && num != v->want_program) continue;
                uint16_t pid = (uint16_t)(rd16(body + k + 2) & 0x1FFFu);
                if (v->want_program == 0) {
                    *v->sel_have = true;
                    *v->sel_version = pat.version;
                    *v->sel_section = pat.section_number;
                }
                /* Several programs may share a PMT PID: a change of program
                 * on the same PID discards the former program's PMT too. */
                if (pid != *v->pmt_pid || num != *v->sel_program) {
                    *v->pmt_pid = pid;
                    *v->sel_program = num;
                    *v->have_pmt = false;
                    *v->video_pid = MOQ_MPEG2TS_PID_NONE;
                    sec_forget(v->pmt_s);
                }
                break;
            }
        }
        return;
    }
    if (*v->pmt_pid > MOQ_MPEG2TS_PID_MAX || p->pid != *v->pmt_pid) return;
    if (sec_push(v->pmt_s, p) != MOQ_OK) return;
    while (sec_next(v->pmt_s, v->pmt_store, MOQ_MPEG2TS_SECTION_MAX, &sec)) {
        if (sec.len < 1 || sec.data[0] != 0x02) continue;    /* private */
        moq_mpeg2ts_pmt_t pmt;
        moq_mpeg2ts_pmt_init(&pmt, NULL, 0);
        moq_result_t rc = moq_mpeg2ts_parse_pmt(sec, &pmt);
        if ((rc != MOQ_OK && rc != MOQ_ERR_BUFFER) || !pmt.current_next)
            continue;
        if (pmt.program_number != *v->sel_program)
            continue;                 /* another program on a shared PID */
        *v->have_pmt = true;
        *v->video_pid = pmt_first_video(sec);
        if (v->pcr_pid) *v->pcr_pid = pmt.pcr_pid;
    }
}

/* The random access selector in force: AUTO becomes the PMT's video PID
 * once the PMT is known (any PID when it lists no video). */
static uint16_t ra_resolve(uint16_t sel, bool have_pmt, uint16_t video_pid)
{
    if (sel != MOQ_MPEG2TS_PID_AUTO || !have_pmt) return sel;
    return video_pid;          /* PID_NONE when the PMT lists no video */
}

/*
 * One step of random access detection. random_access_indicator announces
 * that the next PES packet to start on that PID begins at an access point
 * (2.4.3.5); it may sit on the packet that starts the PES or on an earlier
 * one. Returns true on the packet that starts that PES, when its PID passes
 * the selector (a PID, PID_NONE for any, PID_AUTO for a video stream_id).
 */
static bool rap_step(uint8_t *armed, const moq_mpeg2ts_packet_t *p,
                     uint16_t sel)
{
    if (p->pid == MOQ_MPEG2TS_PID_NULL) return false;
    bool pid_ok = sel == MOQ_MPEG2TS_PID_NONE || sel == MOQ_MPEG2TS_PID_AUTO ||
                  sel == p->pid;
    if (!p->payload_unit_start) {
        /* Each PID keeps its own announcement: one PID's does not cancel
         * another's. */
        if (p->random_access && pid_ok) bit_set(armed, p->pid);
        return false;
    }
    bool signalled = p->random_access || bit_get(armed, p->pid);
    bit_clear(armed, p->pid);
    if (!signalled || !pid_ok || !pes_start(p)) return false;
    if (sel == MOQ_MPEG2TS_PID_AUTO) return pes_is_video(p);
    return true;
}

/* -- Object validation --------------------------------------------------- */

void moq_mpeg2ts_object_rules_init_sized(moq_mpeg2ts_object_rules_t *r,
                                         size_t r_size)
{
    size_t n = sized_clear(r, r_size, sizeof(*r));
    if (COVERS(n, moq_mpeg2ts_object_rules_t, packet_size))
        r->packet_size = MOQ_MPEG2TS_TS_PACKET_SIZE;
}

void moq_mpeg2ts_object_report_init_sized(moq_mpeg2ts_object_report_t *r,
                                          size_t r_size)
{
    (void)sized_clear(r, r_size, sizeof(*r));
}

static moq_result_t validate(uint32_t ps, bool es_packets, uint16_t es_pid,
                             moq_bytes_t payload,
                             moq_mpeg2ts_object_report_t *rep)
{
    rep->packet_count = payload.len / ps;
    rep->bad_packet = 0;
    if (payload.len == 0) {
        rep->reason = MOQ_MPEG2TS_ERR_EMPTY;
        return MOQ_ERR_PROTO;
    }
    if (payload.len % ps != 0) {
        rep->reason = MOQ_MPEG2TS_ERR_PARTIAL_PACKET;
        rep->bad_packet = rep->packet_count;
        return MOQ_ERR_PROTO;
    }
    size_t off = ts_offset(ps);
    for (size_t i = 0; i < rep->packet_count; i++) {
        const uint8_t *t = payload.data + i * ps + off;
        if (t[0] != MOQ_MPEG2TS_SYNC_BYTE) {
            rep->reason = MOQ_MPEG2TS_ERR_SYNC_BYTE;
            rep->bad_packet = i;
            return MOQ_ERR_PROTO;
        }
        if (es_packets) {
            uint16_t pid = pkt_pid(t);
            if (pid == MOQ_MPEG2TS_PID_NULL) {
                rep->reason = MOQ_MPEG2TS_ERR_NULL_PACKET;
                rep->bad_packet = i;
                return MOQ_ERR_PROTO;
            }
            if (pid != es_pid) {
                rep->reason = MOQ_MPEG2TS_ERR_FOREIGN_PID;
                rep->bad_packet = i;
                return MOQ_ERR_PROTO;
            }
        }
    }
    rep->reason = MOQ_MPEG2TS_OK;
    return MOQ_OK;
}

moq_result_t moq_mpeg2ts_validate_object(const moq_mpeg2ts_object_rules_t *rules,
                                         moq_bytes_t payload,
                                         moq_mpeg2ts_object_report_t *report,
                                         size_t report_size)
{
    if (!rules || !report || (!payload.data && payload.len)) return MOQ_ERR_INVAL;
    if (report_size < MOQ_MPEG2TS_OBJECT_REPORT_V0_SIZE) return MOQ_ERR_INVAL;
    if (rules->struct_size < RULES_V0 || !packet_size_ok(rules->packet_size) ||
        (rules->es_packets && rules->es_pid > MOQ_MPEG2TS_PID_MAX))
        return MOQ_ERR_INVAL;
    moq_mpeg2ts_object_report_t rep;
    moq_mpeg2ts_object_report_init(&rep);
    moq_result_t rc = validate(rules->packet_size, rules->es_packets,
                               rules->es_pid, payload, &rep);
    sized_out(report, report_size, &rep, sizeof(rep));
    return rc;
}

/* -- Inspection ---------------------------------------------------------- */

void moq_mpeg2ts_inspect_cfg_init_sized(moq_mpeg2ts_inspect_cfg_t *p,
                                           size_t p_size)
{
    typedef moq_mpeg2ts_inspect_cfg_t T;
    size_t n = sized_clear(p, p_size, sizeof(*p));
    if (COVERS(n, T, packet_size)) p->packet_size = MOQ_MPEG2TS_TS_PACKET_SIZE;
    if (COVERS(n, T, pmt_pid)) p->pmt_pid = MOQ_MPEG2TS_PID_NONE;
    if (COVERS(n, T, timing_pid)) p->timing_pid = MOQ_MPEG2TS_PID_NONE;
    if (COVERS(n, T, pcr_pid)) p->pcr_pid = MOQ_MPEG2TS_PID_NONE;
    if (COVERS(n, T, random_access_pid))
        p->random_access_pid = MOQ_MPEG2TS_PID_NONE;
}

void moq_mpeg2ts_object_info_init_sized(moq_mpeg2ts_object_info_t *i,
                                        size_t i_size)
{
    typedef moq_mpeg2ts_object_info_t T;
    size_t n = sized_clear(i, i_size, sizeof(*i));
    if (COVERS(n, T, pmt_pid)) i->pmt_pid = MOQ_MPEG2TS_PID_NONE;
    if (COVERS(n, T, pmt_pcr_pid)) i->pmt_pcr_pid = MOQ_MPEG2TS_PID_NONE;
    if (COVERS(n, T, pmt_video_pid)) i->pmt_video_pid = MOQ_MPEG2TS_PID_NONE;
    if (COVERS(n, T, timing_pid)) i->timing_pid = MOQ_MPEG2TS_PID_NONE;
    if (COVERS(n, T, pcr_pid)) i->pcr_pid = MOQ_MPEG2TS_PID_NONE;
    if (COVERS(n, T, random_access_pid))
        i->random_access_pid = MOQ_MPEG2TS_PID_NONE;
}

/* Parse a verified PMT section straight into the caller's struct, bounded by
 * its own table, then clear the spans that would point into scratch. */
static bool pmt_copy_out(moq_mpeg2ts_pmt_t *dst, moq_bytes_t sec)
{
    moq_mpeg2ts_stream_t *keep = dst->streams;
    size_t kcap = dst->stream_cap;
    moq_mpeg2ts_pmt_init(dst, keep, kcap);
    moq_result_t rc = moq_mpeg2ts_parse_pmt(sec, dst);
    dst->program_descriptors.data = NULL;
    dst->program_descriptors.len = 0;
    size_t written = dst->stream_count < dst->stream_cap ? dst->stream_count
                                                        : dst->stream_cap;
    for (size_t i = 0; i < written; i++) {
        dst->streams[i].descriptors.data = NULL;
        dst->streams[i].descriptors.len = 0;
    }
    return rc == MOQ_ERR_BUFFER;
}

/* The inspection into a whole info of this build; arguments checked. */
static moq_result_t inspect_full(const moq_mpeg2ts_inspect_cfg_t *ip,
                                 moq_bytes_t payload,
                                 moq_mpeg2ts_object_info_t *info);

moq_result_t moq_mpeg2ts_inspect_object(const moq_mpeg2ts_inspect_cfg_t *ip,
                                        moq_bytes_t payload,
                                        moq_mpeg2ts_object_info_t *info,
                                        size_t info_size)
{
    if (!ip || !info || (!payload.data && payload.len)) return MOQ_ERR_INVAL;
    if (info_size < MOQ_MPEG2TS_OBJECT_INFO_V0_SIZE) return MOQ_ERR_INVAL;
    if (ip->struct_size < INSPECT_V0) return MOQ_ERR_INVAL;
    moq_mpeg2ts_object_info_t full;
    moq_result_t rc = inspect_full(ip, payload, &full);
    if (rc == MOQ_OK || rc == MOQ_ERR_BUFFER)
        sized_out(info, info_size, &full, sizeof(full));
    return rc;
}

static moq_result_t inspect_full(const moq_mpeg2ts_inspect_cfg_t *ip,
                                 moq_bytes_t payload,
                                 moq_mpeg2ts_object_info_t *info)
{
    uint32_t ps = ip->packet_size;
    if (!packet_size_ok(ps) || payload.len % ps != 0) return MOQ_ERR_INVAL;
    if (!pid_sel_ok(ip->timing_pid) || !pid_sel_ok(ip->random_access_pid))
        return MOQ_ERR_INVAL;
    moq_mpeg2ts_object_info_init(info);
    size_t count = payload.len / ps;
    info->packet_count = count;
    bool buffer_short = false;

    /* Pass 1: PSI. A PAT selects the PMT PID unless the caller named it. */
    uint8_t pat_store[MOQ_MPEG2TS_SECTION_MAX];
    uint8_t pmt_store[MOQ_MPEG2TS_SECTION_MAX];
    moq_mpeg2ts_section_state_t pat_s, pmt_s;
    sec_forget(&pat_s);
    sec_forget(&pmt_s);
    uint16_t pmt_pid = ip->pmt_pid;
    /* The program whose PMT is read: the caller's, or the reference program
     * selected as the receiver selects it (psi_feed). */
    uint16_t want_program = ip->program_number;
    bool sel_have = false;
    uint8_t sel_version = 0, sel_section = 0;
    bool pat_out_done = false;

    /* The PAT is read first so that a PMT earlier in the object than its
     * PAT is still found. */
    for (int pass = 0; pass < 2; pass++)
    for (size_t i = 0; i < count; i++) {
        moq_mpeg2ts_packet_t p;
        moq_bytes_t raw = { payload.data + i * ps, ps };
        if (moq_mpeg2ts_parse_packet(raw, ps, &p) != MOQ_OK) {
            if (pass == 0) info->malformed_packets++;
            continue;
        }
        if (p.pid == MOQ_MPEG2TS_PID_NULL) {
            if (pass == 0) info->null_packets++;
            continue;
        }
        if (pass == 0 && p.scrambling) info->scrambled_packets++;
        moq_bytes_t sec;
        if (pass == 0 && p.pid == MOQ_MPEG2TS_PID_PAT) {
            if (sec_push(&pat_s, &p) != MOQ_OK) info->malformed_packets++;
            while (sec_next(&pat_s, pat_store, sizeof(pat_store), &sec)) {
                moq_mpeg2ts_pat_t pat;
                moq_mpeg2ts_pat_init(&pat, NULL, 0);
                moq_result_t rc = moq_mpeg2ts_parse_pat(sec, &pat);
                if (rc != MOQ_OK && rc != MOQ_ERR_BUFFER) {
                    info->bad_crc_sections++;
                    continue;
                }
                info->has_pat = true;
                if (!pat.current_next) continue;
                /* With no program named, the reference is the first program
                 * of the PAT: the first entry of the lowest-numbered section
                 * of the current version that lists one. A later section,
                 * or a repetition, does not move it; a new version does. */
                if (ip->program_number == 0 && sel_have &&
                    pat.version == sel_version &&
                    pat.section_number > sel_section)
                    continue;
                bool listed = false;
                const uint8_t *body = sec.data + 8;
                size_t blen = sec.len - 12;
                for (size_t k = 0; k < blen; k += 4) {
                    uint16_t num = rd16(body + k);
                    uint16_t pid = (uint16_t)(rd16(body + k + 2) & 0x1FFFu);
                    if (num == 0) continue;
                    if (ip->program_number != 0 && num != ip->program_number)
                        continue;
                    if (ip->pmt_pid == MOQ_MPEG2TS_PID_NONE) pmt_pid = pid;
                    if (ip->program_number == 0) {
                        want_program = num;
                        sel_have = true;
                        sel_version = pat.version;
                        sel_section = pat.section_number;
                    }
                    listed = true;
                    break;
                }
                /* The caller's PAT gets the section the program was taken
                 * from, or the first current section when none lists it. */
                if (ip->pat && (listed || !pat_out_done)) {
                    moq_mpeg2ts_pat_t *o = ip->pat;
                    moq_mpeg2ts_program_t *keep = o->programs;
                    size_t kcap = o->program_cap;
                    moq_mpeg2ts_pat_init(o, keep, kcap);
                    buffer_short = moq_mpeg2ts_parse_pat(sec, o) == MOQ_ERR_BUFFER;
                    pat_out_done = true;
                }
            }
        } else if (pass == 1 && pmt_pid <= MOQ_MPEG2TS_PID_MAX &&
                   p.pid == pmt_pid && !info->has_pmt) {
            if (sec_push(&pmt_s, &p) != MOQ_OK) info->malformed_packets++;
            while (!info->has_pmt &&
                   sec_next(&pmt_s, pmt_store, sizeof(pmt_store), &sec)) {
                if (sec.len < 1 || sec.data[0] != 0x02) continue;  /* private */
                moq_mpeg2ts_pmt_t pmt;
                moq_mpeg2ts_pmt_init(&pmt, NULL, 0);
                moq_result_t rc = moq_mpeg2ts_parse_pmt(sec, &pmt);
                if (rc != MOQ_OK && rc != MOQ_ERR_BUFFER) {
                    info->bad_crc_sections++;
                    continue;
                }
                if (!pmt.current_next) continue;
                if (want_program != 0 && pmt.program_number != want_program)
                    continue;             /* another program on a shared PID */
                info->has_pmt = true;
                info->pmt_pid = pmt_pid;
                info->program_number = pmt.program_number;
                info->pmt_pcr_pid = pmt.pcr_pid;
                info->pmt_video_pid = pmt_first_video(sec);
                if (ip->pmt && pmt_copy_out(ip->pmt, sec)) buffer_short = true;
            }
        }
    }
    info->psi_complete = info->has_pat && info->has_pmt;
    if (!info->has_pmt) info->pmt_pid = pmt_pid;

    /* Pass 2: random access points, PES timing, PCR. */
    uint16_t pcr_pid = ip->pcr_pid;
    if (pcr_pid == MOQ_MPEG2TS_PID_NONE && info->has_pmt)
        pcr_pid = info->pmt_pcr_pid;
    uint16_t ra_sel = ra_resolve(ip->random_access_pid, info->has_pmt,
                                 info->pmt_video_pid);
    uint16_t t_sel = ra_resolve(ip->timing_pid, info->has_pmt,
                                info->pmt_video_pid);
    uint8_t armed[(MOQ_MPEG2TS_PID_MAX + 1u) / 8u];
    memset(armed, 0, sizeof(armed));
    bool first_pts_set = false;
    moq_mpeg2ts_pes_header_t first_pes, rap_pes;
    uint16_t first_pes_pid = MOQ_MPEG2TS_PID_NONE;
    bool rap_pes_set = false;
    memset(&first_pes, 0, sizeof(first_pes));
    memset(&rap_pes, 0, sizeof(rap_pes));

    for (size_t i = 0; i < count; i++) {
        moq_mpeg2ts_packet_t p;
        moq_bytes_t raw = { payload.data + i * ps, ps };
        if (moq_mpeg2ts_parse_packet(raw, ps, &p) != MOQ_OK) continue;
        if (p.pid == MOQ_MPEG2TS_PID_NULL) continue;

        if (p.has_pcr) {
            if (pcr_pid == MOQ_MPEG2TS_PID_NONE) pcr_pid = p.pid;
            if (p.pid == pcr_pid) {
                uint64_t v = moq_mpeg2ts_pcr_27mhz(p.pcr_base, p.pcr_ext);
                if (!info->has_pcr) {
                    info->has_pcr = true;
                    info->first_pcr = v;
                    info->first_pcr_index = i;
                }
                info->last_pcr = v;
                info->last_pcr_index = i;
                info->pcr_count++;
                if (p.discontinuity && !info->pcr_discontinuity) {
                    info->pcr_discontinuity = true;
                    info->pcr_discontinuity_index = i;
                }
            }
        }

        bool rap = rap_step(armed, &p, ra_sel);
        if (rap && !info->has_random_access) {
            info->has_random_access = true;
            info->random_access_index = i;
            info->random_access_pid = p.pid;
        }
        if (!pes_start(&p)) continue;
        moq_mpeg2ts_pes_header_t h;
        bool pes_ok = moq_mpeg2ts_parse_pes_header(p.payload, &h) == MOQ_OK;
        if (!pes_ok) info->malformed_packets++;
        if (!pes_ok || !h.has_pts) continue;
        if (t_sel != MOQ_MPEG2TS_PID_NONE) {
            bool want = t_sel == MOQ_MPEG2TS_PID_AUTO ? pes_is_video(&p)
                                                      : p.pid == t_sel;
            if (want && !first_pts_set) {
                first_pts_set = true;
                first_pes = h;
                first_pes_pid = p.pid;
            }
            continue;
        }
        if (!first_pts_set) {
            first_pts_set = true;
            first_pes = h;
            first_pes_pid = p.pid;
        }
        if (rap && !rap_pes_set && info->random_access_index == i) {
            rap_pes_set = true;
            rap_pes = h;
        }
    }

    info->pcr_pid = info->has_pcr ? pcr_pid : MOQ_MPEG2TS_PID_NONE;
    const moq_mpeg2ts_pes_header_t *t = NULL;
    uint16_t tpid = MOQ_MPEG2TS_PID_NONE;
    if (rap_pes_set) {
        t = &rap_pes;
        tpid = info->random_access_pid;
    } else if (first_pts_set) {
        t = &first_pes;
        tpid = first_pes_pid;
    }
    if (t) {
        info->timing_pid = tpid;
        info->has_pts = t->has_pts;
        info->pts = t->pts;
        info->has_dts = t->has_dts;
        info->dts = t->dts;
    }
    return buffer_short ? MOQ_ERR_BUFFER : MOQ_OK;
}

/* -- Receiver ------------------------------------------------------------ */

void moq_mpeg2ts_receiver_cfg_init_sized(moq_mpeg2ts_receiver_cfg_t *c,
                                         size_t c_size)
{
    typedef moq_mpeg2ts_receiver_cfg_t T;
    size_t n = sized_clear(c, c_size, sizeof(*c));
    if (COVERS(n, T, packet_size)) c->packet_size = MOQ_MPEG2TS_TS_PACKET_SIZE;
    if (COVERS(n, T, pcr_pid)) c->pcr_pid = MOQ_MPEG2TS_PID_NONE;
    if (COVERS(n, T, random_access_pid))
        c->random_access_pid = MOQ_MPEG2TS_PID_AUTO;
}

void moq_mpeg2ts_receive_result_init_sized(moq_mpeg2ts_receive_result_t *r,
                                           size_t r_size)
{
    (void)sized_clear(r, r_size, sizeof(*r));
}

void moq_mpeg2ts_delivery_init_sized(moq_mpeg2ts_delivery_t *d, size_t d_size)
{
    size_t n = sized_clear(d, d_size, sizeof(*d));
    if (COVERS(n, moq_mpeg2ts_delivery_t, result))
        moq_mpeg2ts_receive_result_init(&d->result);
}

moq_result_t moq_mpeg2ts_receiver_init(moq_mpeg2ts_receiver_t *rx,
                                       size_t rx_size,
                                       const moq_mpeg2ts_receiver_cfg_t *cfg)
{
    if (!rx || !cfg || rx_size < sizeof(*rx)) return MOQ_ERR_INVAL;
    if (cfg->struct_size < RX_CFG_V0) return MOQ_ERR_INVAL;
    if (!packet_size_ok(cfg->packet_size)) return MOQ_ERR_INVAL;
    if (cfg->es_packets && cfg->es_pid > MOQ_MPEG2TS_PID_MAX) return MOQ_ERR_INVAL;
    if (cfg->pcr_pid > MOQ_MPEG2TS_PID_MAX && cfg->pcr_pid != MOQ_MPEG2TS_PID_NONE)
        return MOQ_ERR_INVAL;
    if (!pid_sel_ok(cfg->random_access_pid)) return MOQ_ERR_INVAL;
    if (cfg->reorder_slot_count && !cfg->reorder_slots) return MOQ_ERR_INVAL;
    memset(rx, 0, sizeof(*rx));
    rx->struct_size = sizeof(*rx);
    memcpy(&rx->_cfg, cfg, RX_CFG_V0);
    rx->_cfg.struct_size = sizeof(rx->_cfg);
    for (size_t i = 0; i < cfg->reorder_slot_count; i++)
        cfg->reorder_slots[i].used = false;
    rx->_pcr_pid = cfg->pcr_pid;
    rx->_pmt_pid = MOQ_MPEG2TS_PID_NONE;
    rx->_video_pid = MOQ_MPEG2TS_PID_NONE;
    rx->_pmt_pcr_pid = MOQ_MPEG2TS_PID_NONE;
    return MOQ_OK;
}

static bool rx_ok(const moq_mpeg2ts_receiver_t *rx)
{
    return rx && rx->struct_size == sizeof(*rx);
}

static void rx_note_end(moq_mpeg2ts_receiver_t *rx, uint64_t group,
                        uint64_t final_object)
{
    for (size_t i = 0; i < MOQ_MPEG2TS_RECEIVER_MAX_ENDS; i++) {
        if (rx->_ends[i].used && rx->_ends[i].group == group) {
            rx->_ends[i].final_object = final_object;
            return;
        }
    }
    size_t k = rx->_ends_next % MOQ_MPEG2TS_RECEIVER_MAX_ENDS;
    rx->_ends[k].used = true;
    rx->_ends[k].group = group;
    rx->_ends[k].final_object = final_object;
    rx->_ends_next = k + 1;
}

static bool rx_end_known(const moq_mpeg2ts_receiver_t *rx, uint64_t group,
                         uint64_t *final_object)
{
    for (size_t i = 0; i < MOQ_MPEG2TS_RECEIVER_MAX_ENDS; i++) {
        if (rx->_ends[i].used && rx->_ends[i].group == group) {
            *final_object = rx->_ends[i].final_object;
            return true;
        }
    }
    return false;
}

/* An end record with this final Object marks a Group known to be empty
 * (END_OF_GROUP status at Object 0). */
#define RX_EMPTY_GROUP UINT64_MAX

/* The first Group after `group` not known to be empty. */
static uint64_t rx_next_group(const moq_mpeg2ts_receiver_t *rx,
                              uint64_t group)
{
    uint64_t next = group + 1, final_object;
    for (size_t n = 0; n < MOQ_MPEG2TS_RECEIVER_MAX_ENDS; n++) {
        if (!rx_end_known(rx, next, &final_object) ||
            final_object != RX_EMPTY_GROUP)
            break;
        next++;
    }
    return next;
}

/* Location of the first Object that should follow the last one taken.
 * Groups known to be empty are stepped over. */
static void rx_expected(const moq_mpeg2ts_receiver_t *rx, uint64_t *g,
                        uint64_t *o)
{
    uint64_t final_object;
    if (rx_end_known(rx, rx->_group, &final_object) &&
        final_object != RX_EMPTY_GROUP && rx->_object >= final_object) {
        *g = rx_next_group(rx, rx->_group);
        *o = 0;
    } else {
        *g = rx->_group;
        *o = rx->_object + 1;
    }
}

static void rx_open_run(moq_mpeg2ts_receiver_t *rx, uint64_t g, uint64_t o)
{
    if (rx->_run_open) return;
    rx->_run_open = true;
    rx->_run_reported = false;
    rx->_run_group = g;
    rx->_run_object = o;
    rx->_run_has_pcr = rx->_has_pcr;
    rx->_run_pcr = rx->_last_pcr;
}

static void rx_lose_continuity(moq_mpeg2ts_receiver_t *rx)
{
    rx->_discontinuous = true;
    memset(rx->_armed, 0, sizeof(rx->_armed));
}

/* Walk the packets of an accepted Object: program tracking, PCR, random
 * access. */
static void rx_walk(moq_mpeg2ts_receiver_t *rx, moq_bytes_t payload,
                    moq_mpeg2ts_receive_result_t *res)
{
    uint32_t ps = rx->_cfg.packet_size;
    psi_view_t v = {
        .want_program = rx->_cfg.program_number,
        .pmt_pid = &rx->_pmt_pid,
        .have_pmt = &rx->_have_pmt,
        .video_pid = &rx->_video_pid,
        .pcr_pid = &rx->_pmt_pcr_pid,
        .sel_have = &rx->_sel_have,
        .sel_version = &rx->_sel_version,
        .sel_section = &rx->_sel_section,
        .sel_program = &rx->_sel_program,
        .pat_s = &rx->_pat_s,
        .pmt_s = &rx->_pmt_s,
        .pat_store = rx->_pat_store,
        .pmt_store = rx->_pmt_store,
    };
    bool first_pcr = true;
    for (size_t i = 0; i < payload.len / ps; i++) {
        moq_mpeg2ts_packet_t p;
        moq_bytes_t raw = { payload.data + i * ps, ps };
        if (moq_mpeg2ts_parse_packet(raw, ps, &p) != MOQ_OK) continue;
        psi_feed(&v, &p);

        if (p.has_pcr) {
            uint16_t want = rx->_cfg.pcr_pid;
            if (want == MOQ_MPEG2TS_PID_NONE && rx->_have_pmt)
                want = rx->_pmt_pcr_pid;
            if (want == MOQ_MPEG2TS_PID_NONE) {
                if (rx->_pcr_pid == MOQ_MPEG2TS_PID_NONE) rx->_pcr_pid = p.pid;
                want = rx->_pcr_pid;
            }
            if (p.pid == want) {
                uint64_t pcr = moq_mpeg2ts_pcr_27mhz(p.pcr_base, p.pcr_ext);
                if (first_pcr && rx->_measure_pending) {
                    if (rx->_measure_has_pcr) {
                        res->gap_duration_known = true;
                        res->gap_duration_27mhz =
                            moq_mpeg2ts_pcr_delta(rx->_measure_pcr, pcr);
                    }
                    rx->_measure_pending = false;
                }
                first_pcr = false;
                rx->_has_pcr = true;
                rx->_last_pcr = pcr;
            }
        }

        uint16_t sel = ra_resolve(rx->_cfg.random_access_pid, rx->_have_pmt,
                                  rx->_video_pid);
        if (rap_step(rx->_armed, &p, sel))
            res->random_access = true;
    }
}

static moq_result_t rx_account(moq_mpeg2ts_receiver_t *rx, uint64_t group,
                               uint64_t object, uint32_t flags,
                               moq_bytes_t payload,
                               moq_mpeg2ts_receive_result_t *res)
{
    moq_mpeg2ts_receive_result_init(res);

    /* The stream is rebuilt in ascending Group, Object order. */
    bool gap_here = false, tail_unknown = false;
    uint64_t gg = 0, go = 0;
    if (rx->_started) {
        if (group < rx->_group ||
            (group == rx->_group && object <= rx->_object)) {
            res->out_of_order = true;
            res->discontinuous = rx->_discontinuous;
            return MOQ_ERR_PROTO;
        }
        uint64_t eg, eo, final_object;
        rx_expected(rx, &eg, &eo);
        if (group == eg && object == eo) {
            /* contiguous */
        } else if (object == 0 && group == rx_next_group(rx, rx->_group) &&
                   !rx_end_known(rx, rx->_group, &final_object)) {
            /* The next Group begins while the end of this one is not
             * confirmed. The END_OF_GROUP bit alone does not confirm it:
             * only its subgroup's FIN names the final Object (drafts 16 and
             * 18), so the tail stays uncertain. A confirmed end that was not
             * reached is a known gap. */
            gap_here = true;
            tail_unknown = true;
            gg = eg;
            go = eo;
        } else {
            gap_here = true;
            gg = eg;
            go = eo;
        }
    } else {
        res->joined_mid_group = object != 0;
    }

    moq_mpeg2ts_object_report_t rep;
    moq_mpeg2ts_object_report_init(&rep);
    bool valid = validate(rx->_cfg.packet_size, rx->_cfg.es_packets,
                          rx->_cfg.es_pid, payload, &rep) == MOQ_OK;

    /* The Location is consumed either way. */
    rx->_started = true;
    rx->_group = group;
    rx->_object = object;
    if (flags & MOQ_MPEG2TS_OBJ_LAST_IN_GROUP) rx_note_end(rx, group, object);

    if (gap_here && tail_unknown) {
        /* Report the possible tail once; nothing is known to be lost. */
        res->gap = true;
        res->gap_tail_unknown = true;
        res->gap_group = gg;
        res->gap_object = go;
    } else if (gap_here) {
        rx_open_run(rx, gg, go);
        if (!rx->_run_reported) {
            res->gap = true;
            res->gap_group = rx->_run_group;
            res->gap_object = rx->_run_object;
            rx->_run_reported = true;
        }
        rx_lose_continuity(rx);
    }

    if (!valid) {
        res->reason = rep.reason;
        /* A rejected Object is a missing one: it starts a run, or extends
         * the one already open, whose first Location is kept. */
        rx_open_run(rx, group, object);
        rx_lose_continuity(rx);
        res->discontinuous = true;
        return MOQ_ERR_PROTO;
    }

    if (rx->_run_open) {
        if (!rx->_run_reported) {
            /* A known loss outranks an uncertain tail behind it. */
            res->gap = true;
            res->gap_tail_unknown = false;
            res->gap_group = rx->_run_group;
            res->gap_object = rx->_run_object;
        }
        rx->_run_open = false;
        rx->_measure_pending = true;
        rx->_measure_has_pcr = rx->_run_has_pcr;
        rx->_measure_pcr = rx->_run_pcr;
    }

    rx_walk(rx, payload, res);
    res->accepted = true;
    if (res->random_access) rx->_discontinuous = false;
    res->discontinuous = rx->_discontinuous;
    return MOQ_OK;
}

moq_result_t moq_mpeg2ts_receiver_push(moq_mpeg2ts_receiver_t *rx,
                                       uint64_t group, uint64_t object,
                                       uint32_t flags, moq_bytes_t payload,
                                       moq_mpeg2ts_receive_result_t *res,
                                       size_t res_size)
{
    if (!rx_ok(rx) || !res || (!payload.data && payload.len)) return MOQ_ERR_INVAL;
    if (res_size < MOQ_MPEG2TS_RECEIVE_RESULT_V0_SIZE) return MOQ_ERR_INVAL;
    moq_mpeg2ts_receive_result_t full;
    moq_result_t rc = rx_account(rx, group, object, flags, payload, &full);
    sized_out(res, res_size, &full, sizeof(full));
    return rc;
}

static bool loc_lt(uint64_t ga, uint64_t oa, uint64_t gb, uint64_t ob)
{
    return ga < gb || (ga == gb && oa < ob);
}

void moq_mpeg2ts_receiver_end_group(moq_mpeg2ts_receiver_t *rx, uint64_t group)
{
    if (!rx_ok(rx)) return;
    bool any = false;
    uint64_t final_object = 0;
    if (rx->_started && rx->_group == group) {
        any = true;
        final_object = rx->_object;
    }
    for (size_t i = 0; i < rx->_cfg.reorder_slot_count; i++) {
        const moq_mpeg2ts_reorder_slot_t *s = &rx->_cfg.reorder_slots[i];
        if (s->used && s->group == group && (!any || s->object > final_object)) {
            any = true;
            final_object = s->object;
        }
    }
    /* An end already known (an END_OF_GROUP status names its Group's final
     * Object exactly, even when that Object never arrived) is not moved
     * back to an earlier Object by the subgroup's FIN behind it. */
    uint64_t known;
    if (rx_end_known(rx, group, &known) &&
        (known == RX_EMPTY_GROUP || known >= final_object))
        return;
    if (any) rx_note_end(rx, group, final_object);
}

void moq_mpeg2ts_receiver_end_group_at(moq_mpeg2ts_receiver_t *rx,
                                       uint64_t group, uint64_t end_object)
{
    if (!rx_ok(rx)) return;
    rx_note_end(rx, group, end_object == 0 ? RX_EMPTY_GROUP : end_object - 1);
}

moq_result_t moq_mpeg2ts_receiver_offer(moq_mpeg2ts_receiver_t *rx,
                                        uint64_t now_us, uint64_t group,
                                        uint64_t object, uint32_t flags,
                                        moq_bytes_t payload, void *ctx)
{
    if (!rx_ok(rx) || (!payload.data && payload.len) ||
        rx->_cfg.reorder_slot_count == 0)
        return MOQ_ERR_INVAL;
    if (rx->_started && !loc_lt(rx->_group, rx->_object, group, object))
        return MOQ_ERR_PROTO;                         /* late */
    moq_mpeg2ts_reorder_slot_t *free_slot = NULL;
    for (size_t i = 0; i < rx->_cfg.reorder_slot_count; i++) {
        moq_mpeg2ts_reorder_slot_t *s = &rx->_cfg.reorder_slots[i];
        if (!s->used) {
            if (!free_slot) free_slot = s;
        } else if (s->group == group && s->object == object) {
            return MOQ_ERR_PROTO;                     /* duplicate */
        }
    }
    if (!free_slot) return MOQ_ERR_BUFFER;
    free_slot->used = true;
    free_slot->flags = flags;
    free_slot->group = group;
    free_slot->object = object;
    free_slot->arrival_us = now_us;
    free_slot->payload = payload;
    free_slot->ctx = ctx;
    if (flags & MOQ_MPEG2TS_OBJ_LAST_IN_GROUP) rx_note_end(rx, group, object);
    return MOQ_OK;
}

static moq_mpeg2ts_reorder_slot_t *rx_head(const moq_mpeg2ts_receiver_t *rx,
                                           size_t *used)
{
    moq_mpeg2ts_reorder_slot_t *head = NULL;
    *used = 0;
    for (size_t i = 0; i < rx->_cfg.reorder_slot_count; i++) {
        moq_mpeg2ts_reorder_slot_t *s = &rx->_cfg.reorder_slots[i];
        if (!s->used) continue;
        (*used)++;
        if (!head || loc_lt(s->group, s->object, head->group, head->object))
            head = s;
    }
    return head;
}

/* How long a held Object may wait: the window, or for a joinable Object of
 * a receiver that has not started, the join grace bounded by the window. */
static uint64_t rx_wait_us(const moq_mpeg2ts_receiver_t *rx,
                           const moq_mpeg2ts_reorder_slot_t *s)
{
    uint64_t w = rx->_cfg.reorder_window_us;
    if (!rx->_started && (s->flags & MOQ_MPEG2TS_OBJ_JOINABLE) &&
        MOQ_MPEG2TS_JOIN_GRACE_US < w)
        w = MOQ_MPEG2TS_JOIN_GRACE_US;
    return w;
}

/* The earliest arrival of a held Object plus its wait, saturating: the one
 * expiry rule shared by poll() and deadline(). */
static uint64_t rx_expiry(const moq_mpeg2ts_receiver_t *rx)
{
    uint64_t best = UINT64_MAX;
    bool any = false;
    for (size_t i = 0; i < rx->_cfg.reorder_slot_count; i++) {
        const moq_mpeg2ts_reorder_slot_t *s = &rx->_cfg.reorder_slots[i];
        if (!s->used) continue;
        uint64_t w = rx_wait_us(rx, s);
        uint64_t d = s->arrival_us > UINT64_MAX - w ? UINT64_MAX : s->arrival_us + w;
        if (!any || d < best) best = d;
        any = true;
    }
    return best;
}

moq_result_t moq_mpeg2ts_receiver_poll(moq_mpeg2ts_receiver_t *rx,
                                       uint64_t now_us,
                                       moq_mpeg2ts_delivery_t *out,
                                       size_t out_size)
{
    if (!rx_ok(rx) || !out) return MOQ_ERR_INVAL;
    /* Refused before anything is released: the held Object stays. */
    if (out_size < MOQ_MPEG2TS_DELIVERY_V0_SIZE) return MOQ_ERR_INVAL;
    size_t used = 0;
    moq_mpeg2ts_reorder_slot_t *h = rx_head(rx, &used);
    if (!h) return MOQ_DONE;
    bool due = rx->_finishing || used == rx->_cfg.reorder_slot_count ||
               now_us >= rx_expiry(rx);
    if (!due) {
        if (rx->_started) {
            uint64_t eg, eo;
            rx_expected(rx, &eg, &eo);
            due = h->group == eg && h->object == eo;
        } else {
            due = h->object == 0;      /* a joiner may start at a Group */
        }
    }
    if (!due) return MOQ_DONE;
    moq_mpeg2ts_delivery_t d;
    moq_mpeg2ts_delivery_init(&d);
    d.group = h->group;
    d.object = h->object;
    d.flags = h->flags;
    d.payload = h->payload;
    d.ctx = h->ctx;
    h->used = false;
    rx_account(rx, d.group, d.object, d.flags, d.payload, &d.result);
    sized_out(out, out_size, &d, sizeof(d));
    return MOQ_OK;
}

uint64_t moq_mpeg2ts_receiver_next_deadline_us(const moq_mpeg2ts_receiver_t *rx)
{
    if (!rx_ok(rx)) return UINT64_MAX;
    return rx_expiry(rx);
}

void moq_mpeg2ts_receiver_finish(moq_mpeg2ts_receiver_t *rx)
{
    if (rx_ok(rx)) rx->_finishing = true;
}

moq_result_t moq_mpeg2ts_strip_prefix(moq_bytes_t in, uint8_t *out, size_t cap,
                                      size_t *out_len)
{
    if (!out_len || (!in.data && in.len)) return MOQ_ERR_INVAL;
    *out_len = 0;
    const size_t ps = MOQ_MPEG2TS_M2TS_PACKET_SIZE;
    if (in.len % ps != 0) return MOQ_ERR_PROTO;
    size_t n = in.len / ps;
    for (size_t i = 0; i < n; i++)
        if (in.data[i * ps + 4] != MOQ_MPEG2TS_SYNC_BYTE) return MOQ_ERR_PROTO;
    *out_len = n * MOQ_MPEG2TS_TS_PACKET_SIZE;
    if (!out || cap < *out_len) return MOQ_ERR_BUFFER;
    /* Front to back: each destination ends before its source starts. */
    for (size_t i = 0; i < n; i++)
        memmove(out + i * MOQ_MPEG2TS_TS_PACKET_SIZE, in.data + i * ps + 4,
                MOQ_MPEG2TS_TS_PACKET_SIZE);
    return MOQ_OK;
}

/* Two packets are one packet and its duplicate: same PID and counter, a
 * payload, and the same payload octets (2.4.3.3). */
static bool same_payload_packet(const uint8_t *a, const uint8_t *b)
{
    if (pkt_pid(a) != pkt_pid(b) || (a[3] & 0x0Fu) != (b[3] & 0x0Fu) ||
        !pkt_has_payload(a) || !pkt_has_payload(b))
        return false;
    size_t oa = 4, ob = 4;
    if (a[3] & 0x20u) oa = 5u + a[4];
    if (b[3] & 0x20u) ob = 5u + b[4];
    if (oa > MOQ_MPEG2TS_TS_PACKET_SIZE || ob > MOQ_MPEG2TS_TS_PACKET_SIZE ||
        oa != ob)
        return false;
    return memcmp(a + oa, b + ob, MOQ_MPEG2TS_TS_PACKET_SIZE - oa) == 0;
}

moq_result_t moq_mpeg2ts_align_join_psi(uint8_t *psi, size_t len,
                                        uint32_t packet_size,
                                        moq_bytes_t first)
{
    if (!psi || !first.data || !packet_size_ok(packet_size)) return MOQ_ERR_INVAL;
    if (len % packet_size != 0 || first.len % packet_size != 0)
        return MOQ_ERR_PROTO;
    size_t off = ts_offset(packet_size);
    size_t n = len / packet_size, m = first.len / packet_size;
    for (size_t i = 0; i < n; i++)
        if (psi[i * packet_size + off] != MOQ_MPEG2TS_SYNC_BYTE) return MOQ_ERR_PROTO;
    for (size_t j = 0; j < m; j++)
        if (first.data[j * packet_size + off] != MOQ_MPEG2TS_SYNC_BYTE)
            return MOQ_ERR_PROTO;

    for (size_t i = 0; i < n; i++) {
        uint8_t *t = psi + i * packet_size + off;
        uint16_t pid = pkt_pid(t);
        /* Only the last init packet of each PID anchors the count; earlier
         * ones are handled when their PID's last packet is reached. */
        bool later_same = false;
        for (size_t k = i + 1; k < n; k++)
            if (pkt_pid(psi + k * packet_size + off) == pid) later_same = true;
        if (later_same) continue;

        const uint8_t *next = NULL;
        for (size_t j = 0; j < m && !next; j++) {
            const uint8_t *f = first.data + j * packet_size + off;
            if (pkt_pid(f) == pid) next = f;
        }
        if (!next) continue;
        /* The counter advances only on packets that carry a payload. */
        uint8_t cc = (uint8_t)(next[3] & 0x0Fu);
        if (pkt_has_payload(next)) cc = (uint8_t)((cc + 15u) & 0x0Fu);
        /* Walk this PID's init packets backwards from the last. A packet
         * that duplicates the one before it on the PID keeps the counter
         * that packet gets, so it stays a duplicate. */
        for (size_t k = i + 1; k-- > 0;) {
            uint8_t *u = psi + k * packet_size + off;
            if (pkt_pid(u) != pid) continue;
            const uint8_t *prev = NULL;
            for (size_t q = k; q-- > 0;) {
                const uint8_t *c = psi + q * packet_size + off;
                if (pkt_pid(c) == pid) { prev = c; break; }
            }
            bool dup = prev && same_payload_packet(prev, u);
            u[3] = (uint8_t)((u[3] & 0xF0u) | cc);
            if (pkt_has_payload(u) && !dup) cc = (uint8_t)((cc + 15u) & 0x0Fu);
        }
    }
    return MOQ_OK;
}

/* -- Switcher ------------------------------------------------------------- */

moq_result_t moq_mpeg2ts_switcher_init(moq_mpeg2ts_switcher_t *sw,
                                       size_t sw_size, uint32_t packet_size)
{
    if (!sw || sw_size < sizeof(*sw) || !packet_size_ok(packet_size))
        return MOQ_ERR_INVAL;
    memset(sw, 0, sizeof(*sw));
    sw->struct_size = sizeof(*sw);
    sw->_packet_size = packet_size;
    sw->_pids[0].used = true;               /* PID 0: the PAT */
    sw->_pids[0].pid = MOQ_MPEG2TS_PID_PAT;
    return MOQ_OK;
}

static moq_mpeg2ts_switch_pid_t *sw_pid(moq_mpeg2ts_switcher_t *sw, uint16_t pid)
{
    for (size_t i = 0; i < 1 + MOQ_MPEG2TS_SWITCHER_MAX_PMT; i++)
        if (sw->_pids[i].used && sw->_pids[i].pid == pid) return &sw->_pids[i];
    return NULL;
}

static void sw_track_pmt(moq_mpeg2ts_switcher_t *sw, uint16_t pid)
{
    if (pid == MOQ_MPEG2TS_PID_PAT || pid > MOQ_MPEG2TS_PID_MAX ||
        pid == MOQ_MPEG2TS_PID_NULL || sw_pid(sw, pid))
        return;
    for (size_t i = 1; i < 1 + MOQ_MPEG2TS_SWITCHER_MAX_PMT; i++) {
        if (!sw->_pids[i].used) {
            memset(&sw->_pids[i], 0, sizeof(sw->_pids[i]));
            sw->_pids[i].used = true;
            sw->_pids[i].pid = pid;
            return;
        }
    }
    sw->_stats.pmt_pids_untracked++;
}

static moq_mpeg2ts_switch_table_t *sw_table(moq_mpeg2ts_switcher_t *sw,
                                            uint8_t table_id, uint16_t ext)
{
    moq_mpeg2ts_switch_table_t *free_t = NULL;
    for (size_t i = 0; i < 1 + MOQ_MPEG2TS_SWITCHER_MAX_PMT; i++) {
        moq_mpeg2ts_switch_table_t *t = &sw->_tables[i];
        if (t->used && t->table_id == table_id && t->ext == ext) return t;
        if (!t->used && !free_t) free_t = t;
    }
    if (!free_t) return NULL;
    memset(free_t, 0, sizeof(*free_t));
    free_t->used = true;
    free_t->table_id = table_id;
    free_t->ext = ext;
    return free_t;
}

void moq_mpeg2ts_switcher_switch(moq_mpeg2ts_switcher_t *sw,
                                 const uint16_t *pmt_pids, size_t count)
{
    if (!sw || sw->struct_size != sizeof(*sw)) return;
    sw->_stats.switches++;
    sw->_switched = true;
    sw->_pcr_pending = true;
    memset(sw->_seen, 0, sizeof(sw->_seen));
    /* The previous track's PMT PIDs and partial sections are forgotten. */
    for (size_t i = 1; i < 1 + MOQ_MPEG2TS_SWITCHER_MAX_PMT; i++)
        memset(&sw->_pids[i], 0, sizeof(sw->_pids[i]));
    sw->_pids[0].in_section = false;
    sw->_pids[0].have_last = false;
    for (size_t i = 0; i < 1 + MOQ_MPEG2TS_SWITCHER_MAX_PMT; i++)
        sw->_tables[i].offset_set = false;
    for (size_t i = 0; pmt_pids && i < count; i++) sw_track_pmt(sw, pmt_pids[i]);
}

/* Version this section is emitted with (called once per section, at its
 * version octet). */
static uint8_t sw_version(moq_mpeg2ts_switcher_t *sw,
                          moq_mpeg2ts_switch_table_t *t, uint8_t v, bool cur)
{
    uint8_t nv = v;
    if (sw->_switched) {
        if (cur) {
            if (!t->offset_set) {
                t->offset = t->have_out
                          ? (uint8_t)((t->out_version + 1u - v) & 0x1Fu) : 0;
                t->offset_set = true;
            }
            nv = (uint8_t)((v + t->offset) & 0x1Fu);
        } else if (t->offset_set) {
            nv = (uint8_t)((v + t->offset) & 0x1Fu);
        } else if (t->have_out) {
            /* A future table before the new current one: clear of both the
             * last emitted version and the one the current table will take
             * when it follows the source by one. */
            nv = (uint8_t)((t->out_version + 2u) & 0x1Fu);
        }
    }
    if (cur) {
        t->have_out = true;
        t->out_version = nv;
    }
    return nv;
}

/* A PAT section has been seen whole: learn the PMT PIDs it lists. */
static void sw_learn_pat(moq_mpeg2ts_switcher_t *sw, size_t len)
{
    moq_bytes_t sec = { sw->_pat_sec, len };
    moq_mpeg2ts_pat_t pat;
    moq_mpeg2ts_pat_init(&pat, NULL, 0);
    moq_result_t rc = moq_mpeg2ts_parse_pat(sec, &pat);
    if ((rc != MOQ_OK && rc != MOQ_ERR_BUFFER) || !pat.current_next) return;
    for (size_t k = 8; k + 4 <= len - 4; k += 4) {
        if (rd16(sw->_pat_sec + k) == 0) continue;
        sw_track_pmt(sw, (uint16_t)(rd16(sw->_pat_sec + k + 2) & 0x1FFFu));
    }
}

/* Rewrite the octets [pos, end) of the section in progress on one PID. */
static size_t sw_feed(moq_mpeg2ts_switcher_t *sw, moq_mpeg2ts_switch_pid_t *sp,
                      uint8_t *d, size_t pos, size_t end)
{
    while (pos < end && sp->in_section) {
        uint8_t orig = d[pos];
        uint16_t k = sp->sec_pos;
        if (k < 8) sp->hdr[k] = orig;
        if (k == 2) {
            size_t sl = ((size_t)(sp->hdr[1] & 0x0Fu) << 8) | sp->hdr[2];
            if (sl > MOQ_MPEG2TS_SECTION_MAX - 3) {
                sp->in_section = false;        /* cannot find its end */
                sp->sec_len = 0;
                return end;
            }
            sp->sec_len = (uint16_t)(3 + sl);
        }
        if (k == 5) {
            uint8_t want = sp->pid == MOQ_MPEG2TS_PID_PAT ? 0x00 : 0x02;
            bool long_form = (sp->hdr[1] & 0x80u) && sp->sec_len >= 12;
            if (sp->hdr[0] == want && long_form) {
                uint16_t ext = want == 0x02 ? rd16(sp->hdr + 3) : 0;
                moq_mpeg2ts_switch_table_t *t = sw_table(sw, want, ext);
                if (t) {
                    uint8_t v = (uint8_t)((orig >> 1) & 0x1Fu);
                    uint8_t nv = sw_version(sw, t, v, (orig & 1u) != 0);
                    if (nv != v) {
                        sp->rewrite = true;
                        d[pos] = (uint8_t)((orig & 0xC1u) | (nv << 1));
                    }
                }
                sp->pat_copy = want == 0x00;
                if (sp->pat_copy) memcpy(sw->_pat_sec, sp->hdr, 5);
            }
        }
        if (sp->sec_len && k + 4u >= sp->sec_len) {
            /* CRC_32 field: keep the source's error, if any, exactly. */
            if (sp->rewrite) {
                unsigned sh = 24u - 8u * (unsigned)(k + 4u - sp->sec_len);
                d[pos] = (uint8_t)(orig ^ (uint8_t)(sp->crc_orig >> sh) ^
                                   (uint8_t)(sp->crc_new >> sh));
            }
        } else {
            sp->crc_orig = crc_step(sp->crc_orig, orig);
            sp->crc_new = crc_step(sp->crc_new, d[pos]);
        }
        if (sp->pat_copy && k >= 5 && k < MOQ_MPEG2TS_SECTION_MAX)
            sw->_pat_sec[k] = orig;
        sp->sec_pos++;
        pos++;
        if (sp->sec_len && sp->sec_pos == sp->sec_len) {
            if (sp->rewrite) sw->_stats.sections_rewritten++;
            if (sp->pat_copy) sw_learn_pat(sw, sp->sec_len);
            sp->in_section = false;
        }
    }
    return pos;
}

static void sw_section_start(moq_mpeg2ts_switch_pid_t *sp)
{
    sp->in_section = true;
    sp->rewrite = false;
    sp->pat_copy = false;
    sp->sec_len = 0;
    sp->sec_pos = 0;
    sp->crc_orig = 0xFFFFFFFFu;
    sp->crc_new = 0xFFFFFFFFu;
}

/* PSI rewriting of one payload of a tracked PID (pointer already checked). */
static void sw_psi(moq_mpeg2ts_switcher_t *sw, moq_mpeg2ts_switch_pid_t *sp,
                   const moq_mpeg2ts_packet_t *p, uint8_t *d)
{
    size_t n = p->payload.len;
    /* A duplicate repeats its original's bytes exactly (2.4.3.3),
     * payload_unit_start_indicator included; it is rewritten exactly as the
     * original was. Anything else with the same counter is a different
     * packet. */
    if (sp->have_last && sp->last_cc == p->continuity_counter &&
        sp->last_pusi == p->payload_unit_start &&
        sp->last_out_len == n && memcmp(sp->last_in, d, n) == 0) {
        memcpy(d, sp->last_out, n);
        return;
    }
    if (sp->in_section &&
        (!sp->have_last ||
         p->continuity_counter != ((sp->last_cc + 1u) & 0x0Fu)))
        sp->in_section = false;              /* continuation lost */
    sp->have_last = true;
    sp->last_cc = p->continuity_counter;
    sp->last_pusi = p->payload_unit_start;
    memcpy(sp->last_in, d, n);

    size_t pos = 0, cont_end = n;
    if (p->payload_unit_start) {
        pos = 1;
        cont_end = 1 + (size_t)d[0];
    }
    if (sp->in_section) {
        pos = sw_feed(sw, sp, d, pos, cont_end);
        if (p->payload_unit_start) sp->in_section = false;  /* cut short */
    }
    if (p->payload_unit_start) {
        pos = cont_end;
        while (pos < n && d[pos] != 0xFF) {
            sw_section_start(sp);
            pos = sw_feed(sw, sp, d, pos, n);
            if (sp->in_section || sp->sec_len == 0) break;
        }
    }
    sp->last_out_len = (uint8_t)n;
    memcpy(sp->last_out, d, n);
}

moq_result_t moq_mpeg2ts_switcher_process(moq_mpeg2ts_switcher_t *sw,
                                          uint8_t *packet)
{
    if (!sw || !packet || sw->struct_size != sizeof(*sw)) return MOQ_ERR_INVAL;
    uint32_t ps = sw->_packet_size;
    moq_mpeg2ts_packet_t p;
    moq_bytes_t raw = { packet, ps };
    if (moq_mpeg2ts_parse_packet(raw, ps, &p) != MOQ_OK) return MOQ_ERR_PROTO;
    uint8_t *t = packet + ts_offset(ps);
    uint8_t *pl = p.payload.len
                ? packet + (size_t)(p.payload.data - packet) : NULL;

    moq_mpeg2ts_switch_pid_t *sp = sw_pid(sw, p.pid);
    if (sp && pl && p.payload_unit_start && 1u + pl[0] > p.payload.len)
        return MOQ_ERR_PROTO;   /* checked before anything is changed */

    /* Nothing below can fail. */
    if (sp && pl) sw_psi(sw, sp, &p, pl);
    if (sw->_switched && p.pid != MOQ_MPEG2TS_PID_NULL &&
        !bit_get(sw->_seen, p.pid)) {
        bit_set(sw->_seen, p.pid);
        if (p.has_adaptation_field && p.adaptation_field_length >= 1) {
            t[5] |= 0x80u;
            sw->_stats.first_packet_di_set++;
        } else {
            sw->_stats.first_packet_di_skipped++;
        }
    }
    if (p.has_pcr && sw->_pcr_pending) {
        t[5] |= 0x80u;                       /* discontinuity_indicator */
        sw->_pcr_pending = false;
        sw->_stats.pcr_discontinuities_set++;
    }
    return MOQ_OK;
}

moq_result_t moq_mpeg2ts_switcher_get_stats(const moq_mpeg2ts_switcher_t *sw,
                                            moq_mpeg2ts_switcher_stats_t *out,
                                            size_t out_size)
{
    if (!sw || !out || sw->struct_size != sizeof(*sw) || out_size < SW_STATS_V0)
        return MOQ_ERR_INVAL;
    moq_mpeg2ts_switcher_stats_t snap = sw->_stats;
    size_t n = out_size < sizeof(snap) ? out_size : sizeof(snap);
    snap.struct_size = (uint32_t)n;
    memcpy(out, &snap, n);
    return MOQ_OK;
}

/* -- Egress timing --------------------------------------------------------- */

moq_result_t moq_mpeg2ts_pcr_interpolate(uint64_t pcr_a, uint64_t byte_a,
                                         uint64_t pcr_b, uint64_t byte_b,
                                         uint64_t byte_i, uint64_t *pcr_i)
{
    if (!pcr_i || byte_b <= byte_a || byte_i < byte_a) return MOQ_ERR_INVAL;
    uint64_t span = moq_mpeg2ts_pcr_delta(pcr_a, pcr_b);
    uint64_t step = muldiv_sat(byte_i - byte_a, span, byte_b - byte_a);
    *pcr_i = ((pcr_a % MOQ_MPEG2TS_PCR_WRAP) +
              step % MOQ_MPEG2TS_PCR_WRAP) % MOQ_MPEG2TS_PCR_WRAP;
    return MOQ_OK;
}

uint64_t moq_mpeg2ts_stuffing_packets(uint64_t mux_rate_bps,
                                      uint64_t elapsed_27mhz,
                                      uint64_t packets_sent)
{
    /* packets due = rate * elapsed / (188 * 8 * 27e6) */
    uint64_t due = muldiv_sat(mux_rate_bps, elapsed_27mhz,
                              UINT64_C(1504) * UINT64_C(27000000));
    return due > packets_sent ? due - packets_sent : 0;
}

moq_result_t moq_mpeg2ts_null_packet(uint32_t packet_size, uint8_t *out,
                                     size_t cap)
{
    if (!out || !packet_size_ok(packet_size)) return MOQ_ERR_INVAL;
    if (cap < packet_size) return MOQ_ERR_BUFFER;
    size_t off = ts_offset(packet_size);
    memset(out, 0, off);
    uint8_t *t = out + off;
    memset(t, 0xFF, MOQ_MPEG2TS_TS_PACKET_SIZE);
    t[0] = MOQ_MPEG2TS_SYNC_BYTE;
    t[1] = 0x1F;
    t[2] = 0xFF;
    t[3] = 0x10;
    return MOQ_OK;
}

/* -- Per-program filter ---------------------------------------------------- */

void moq_mpeg2ts_program_filter_cfg_init_sized(
    moq_mpeg2ts_program_filter_cfg_t *c, size_t c_size)
{
    typedef moq_mpeg2ts_program_filter_cfg_t T;
    size_t n = sized_clear(c, c_size, sizeof(*c));
    if (COVERS(n, T, packet_size)) c->packet_size = MOQ_MPEG2TS_TS_PACKET_SIZE;
    if (COVERS(n, T, keep_cat)) c->keep_cat = true;
}

/* PIDs kept regardless of the PMT: SI, and the program's PMT. */
static void keep_fixed(moq_mpeg2ts_program_filter_t *f)
{
    memset(f->_keep, 0, sizeof(f->_keep));
    for (size_t i = 0; i < f->_si_pid_count; i++) bit_set(f->_keep, f->_si_pids[i]);
    if (f->_program_present) bit_set(f->_keep, f->_pmt_pid);
}

moq_result_t moq_mpeg2ts_program_filter_init(
    moq_mpeg2ts_program_filter_t *f, size_t f_size,
    const moq_mpeg2ts_program_filter_cfg_t *cfg)
{
    if (!f || !cfg || f_size < sizeof(*f)) return MOQ_ERR_INVAL;
    if (cfg->struct_size < FILTER_CFG_V0) return MOQ_ERR_INVAL;
    if (!packet_size_ok(cfg->packet_size) || cfg->program_number == 0)
        return MOQ_ERR_INVAL;
    if (cfg->si_pid_count > MOQ_MPEG2TS_FILTER_MAX_SI_PIDS ||
        (cfg->si_pid_count && !cfg->si_pids))
        return MOQ_ERR_INVAL;
    for (size_t i = 0; i < cfg->si_pid_count; i++)
        if (cfg->si_pids[i] > MOQ_MPEG2TS_PID_MAX ||
            cfg->si_pids[i] == MOQ_MPEG2TS_PID_PAT ||
            cfg->si_pids[i] == MOQ_MPEG2TS_PID_NULL)
            return MOQ_ERR_INVAL;
    memset(f, 0, sizeof(*f));
    f->struct_size = sizeof(*f);
    f->_packet_size = cfg->packet_size;
    f->_program_number = cfg->program_number;
    f->_keep_null = cfg->keep_null;
    f->_keep_cat = cfg->keep_cat;
    f->_si_pid_count = cfg->si_pid_count;
    for (size_t i = 0; i < cfg->si_pid_count; i++) f->_si_pids[i] = cfg->si_pids[i];
    f->_pmt_pid = MOQ_MPEG2TS_PID_NONE;
    keep_fixed(f);
    return MOQ_OK;
}

bool moq_mpeg2ts_program_filter_program_gone(const moq_mpeg2ts_program_filter_t *f)
{
    return f && f->_program_gone;
}

static void mask_set(uint8_t *mask, uint8_t i)
{
    mask[i >> 3] = (uint8_t)(mask[i >> 3] | (1u << (i & 7u)));
}

/* Sections 0..last all seen. */
static bool mask_full(const uint8_t *mask, uint8_t last)
{
    for (unsigned i = 0; i <= last; i++)
        if (!((mask[i >> 3] >> (i & 7u)) & 1u)) return false;
    return true;
}

/* Build the rewritten PAT packet for the program on pmt_pid. */
static bool filter_emit_pat(moq_mpeg2ts_program_filter_t *f, uint16_t tsid,
                            uint16_t pmt_pid, const uint8_t *src_packet,
                            uint8_t *out)
{
    /* Per-Program: version changes exactly when the rewritten content does. */
    if (f->_pat_emitted && (tsid != f->_pat_tsid || pmt_pid != f->_pat_pmt_pid))
        f->_pat_version = (uint8_t)((f->_pat_version + 1u) & 0x1Fu);
    f->_pat_tsid = tsid;
    f->_pat_pmt_pid = pmt_pid;

    moq_mpeg2ts_program_t prog = { f->_program_number, pmt_pid };
    uint8_t s[16];
    size_t slen = 0;
    if (moq_mpeg2ts_build_pat(tsid, f->_pat_version, &prog, 1, s, sizeof(s),
                              &slen) != MOQ_OK)
        return false;
    uint8_t cc = f->_pat_cc;
    uint8_t tmp[MOQ_MPEG2TS_M2TS_PACKET_SIZE];
    size_t plen = 0;
    moq_bytes_t sb = { s, slen };
    if (moq_mpeg2ts_packetize_section(MOQ_MPEG2TS_PID_PAT, &cc, sb,
                                      f->_packet_size, tmp, sizeof(tmp),
                                      &plen) != MOQ_OK || plen != f->_packet_size)
        return false;
    /* Keep the source packet's prefix on a 192-octet track. */
    if (f->_packet_size == MOQ_MPEG2TS_M2TS_PACKET_SIZE)
        memcpy(tmp, src_packet, MOQ_MPEG2TS_PREFIX_SIZE);
    memcpy(out, tmp, f->_packet_size);
    f->_pat_cc = cc;
    f->_pat_emitted = true;
    return true;
}

/* One complete PAT section. Returns true when it lists the program in the
 * current table (*pmt_pid, *tsid set). */
static bool filter_on_pat(moq_mpeg2ts_program_filter_t *f, moq_bytes_t sec,
                          uint16_t *tsid, uint16_t *pmt_pid)
{
    moq_mpeg2ts_pat_t pat;
    moq_mpeg2ts_pat_init(&pat, NULL, 0);
    moq_result_t rc = moq_mpeg2ts_parse_pat(sec, &pat);
    if (rc != MOQ_OK && rc != MOQ_ERR_BUFFER) return false;
    if (!pat.current_next) return false;    /* not yet applicable */

    if (!f->_pat_building || pat.version != f->_pat_build_version ||
        pat.last_section_number != f->_pat_build_last) {
        f->_pat_building = true;
        f->_pat_build_version = pat.version;
        f->_pat_build_last = pat.last_section_number;
        f->_pat_build_found = false;
        memset(f->_pat_build_mask, 0, sizeof(f->_pat_build_mask));
    }
    mask_set(f->_pat_build_mask, pat.section_number);

    uint16_t found = MOQ_MPEG2TS_PID_NONE;
    const uint8_t *body = sec.data + 8;
    for (size_t k = 0; k + 4 <= sec.len - 12; k += 4) {
        if (rd16(body + k) == f->_program_number) {
            found = (uint16_t)(rd16(body + k + 2) & 0x1FFFu);
            break;
        }
    }
    if (found != MOQ_MPEG2TS_PID_NONE) {
        f->_pat_build_found = true;
        if (!f->_program_present) {
            f->_have_pmt = false;
            sec_forget(&f->_pmt_s);
            f->_program_present = true;
            f->_pmt_pid = found;
            keep_fixed(f);
        } else if (found != f->_pmt_pid) {
            /* The PMT moved: the elementary streams of the PMT in force
             * stay until the PMT on the new PID replaces them. */
            if (!pid_listed(f->_si_pids, f->_si_pid_count, f->_pmt_pid))
                f->_keep[f->_pmt_pid >> 3] =
                    (uint8_t)(f->_keep[f->_pmt_pid >> 3] &
                              ~(1u << (f->_pmt_pid & 7u)));
            f->_have_pmt = false;
            sec_forget(&f->_pmt_s);
            f->_pmt_pid = found;
            bit_set(f->_keep, found);
        }
        f->_program_gone = false;
        *tsid = pat.transport_stream_id;
        *pmt_pid = found;
        return true;
    }
    /* Absence counts only once the whole current table has been seen. */
    if (mask_full(f->_pat_build_mask, f->_pat_build_last) &&
        !f->_pat_build_found && f->_program_present) {
        f->_program_present = false;
        f->_program_gone = true;
        f->_have_pmt = false;
        sec_forget(&f->_pmt_s);
        keep_fixed(f);
    }
    return false;
}

static void filter_on_pmt(moq_mpeg2ts_program_filter_t *f, moq_bytes_t sec)
{
    if (sec.len < 1 || sec.data[0] != 0x02) return;   /* private section */
    moq_mpeg2ts_pmt_t pmt;
    moq_mpeg2ts_pmt_init(&pmt, NULL, 0);
    moq_result_t rc = moq_mpeg2ts_parse_pmt(sec, &pmt);
    if (rc != MOQ_OK && rc != MOQ_ERR_BUFFER) return;
    if (!pmt.current_next || pmt.program_number != f->_program_number) return;
    if (f->_have_pmt && pmt.version == f->_pmt_version) return;

    keep_fixed(f);
    f->_have_pmt = true;
    f->_pmt_version = pmt.version;
    f->_pcr_pid = pmt.pcr_pid;
    bit_set(f->_keep, pmt.pcr_pid);
    map_ca_pids(f->_keep, pmt.program_descriptors.data,
                pmt.program_descriptors.len);
    const uint8_t *body = sec.data + 8;
    size_t blen = sec.len - 12;
    for (size_t i = 4 + pmt.program_descriptors.len; i + 5 <= blen;) {
        size_t eil = rd16(body + i + 3) & 0x0FFFu;
        bit_set(f->_keep, (uint16_t)(rd16(body + i + 1) & 0x1FFFu));
        map_ca_pids(f->_keep, body + i + 5, eil);
        i += 5 + eil;
    }
}

/* One complete CAT section (2.4.4.6): collect the EMM PIDs it names. */
static void filter_on_cat(moq_mpeg2ts_program_filter_t *f, moq_bytes_t sec)
{
    const uint8_t *body;
    size_t blen;
    if (section_check(sec, 0x01, &body, &blen) != MOQ_OK) return;
    const uint8_t *p = sec.data;
    if (!(p[5] & 1u)) return;                /* future table */
    uint8_t version = (uint8_t)((p[5] >> 1) & 0x1Fu);
    if (!f->_cat_building || version != f->_cat_build_version ||
        p[7] != f->_cat_build_last) {
        f->_cat_building = true;
        f->_cat_build_version = version;
        f->_cat_build_last = p[7];
        memset(f->_cat_build_mask, 0, sizeof(f->_cat_build_mask));
        memset(f->_emm_next, 0, sizeof(f->_emm_next));
    }
    mask_set(f->_cat_build_mask, p[6]);
    map_ca_pids(f->_emm_next, body, blen);
    /* A complete version replaces the one in force; until then both pass. */
    if (mask_full(f->_cat_build_mask, f->_cat_build_last))
        memcpy(f->_emm, f->_emm_next, sizeof(f->_emm));
}

moq_result_t moq_mpeg2ts_program_filter_push(moq_mpeg2ts_program_filter_t *f,
                                             moq_bytes_t in, uint8_t *out,
                                             size_t out_cap,
                                             moq_mpeg2ts_filter_action_t *action)
{
    if (!f || !out || !action || f->struct_size != sizeof(*f)) return MOQ_ERR_INVAL;
    *action = MOQ_MPEG2TS_FILTER_DROP;
    uint32_t ps = f->_packet_size;
    if (out_cap < ps || !in.data || in.len != ps) return MOQ_ERR_INVAL;
    moq_mpeg2ts_packet_t p;
    if (moq_mpeg2ts_parse_packet(in, ps, &p) != MOQ_OK) return MOQ_ERR_PROTO;
    moq_bytes_t sec;

    if (p.pid == MOQ_MPEG2TS_PID_PAT) {
        bool emit = false;
        uint16_t tsid = 0, pmt_pid = MOQ_MPEG2TS_PID_NONE;
        if (sec_push(&f->_pat_s, &p) == MOQ_OK) {
            while (sec_next(&f->_pat_s, f->_pat_storage,
                            sizeof(f->_pat_storage), &sec)) {
                uint16_t t = 0, pp = MOQ_MPEG2TS_PID_NONE;
                if (filter_on_pat(f, sec, &t, &pp)) {
                    emit = true;
                    tsid = t;
                    pmt_pid = pp;
                }
            }
        }
        /* One rewritten PAT at most per source packet. */
        if (emit && filter_emit_pat(f, tsid, pmt_pid, in.data, out))
            *action = MOQ_MPEG2TS_FILTER_PAT;
        return MOQ_OK;
    }
    if (p.pid == MOQ_MPEG2TS_PID_NULL) {
        if (f->_keep_null) {
            memcpy(out, in.data, ps);
            *action = MOQ_MPEG2TS_FILTER_PASS;
        }
        return MOQ_OK;
    }
    if (p.pid == MOQ_MPEG2TS_PID_CAT) {
        if (sec_push(&f->_cat_s, &p) == MOQ_OK)
            while (sec_next(&f->_cat_s, f->_cat_storage,
                            sizeof(f->_cat_storage), &sec))
                filter_on_cat(f, sec);
        if (f->_keep_cat) {
            memcpy(out, in.data, ps);
            *action = MOQ_MPEG2TS_FILTER_PASS;
        }
        return MOQ_OK;
    }
    if (f->_program_present && p.pid == f->_pmt_pid) {
        if (sec_push(&f->_pmt_s, &p) == MOQ_OK)
            while (sec_next(&f->_pmt_s, f->_pmt_storage,
                            sizeof(f->_pmt_storage), &sec))
                filter_on_pmt(f, sec);
        memcpy(out, in.data, ps);
        *action = MOQ_MPEG2TS_FILTER_PASS;
        return MOQ_OK;
    }
    bool keep = (bit_get(f->_keep, p.pid) && (f->_program_present ||
                                               !f->_pat_building)) ||
                (f->_keep_cat && (bit_get(f->_emm, p.pid) ||
                                  bit_get(f->_emm_next, p.pid)));
    if (keep) {
        memcpy(out, in.data, ps);
        *action = MOQ_MPEG2TS_FILTER_PASS;
    }
    return MOQ_OK;
}

/* -- Packetizer ------------------------------------------------------------ */

void moq_mpeg2ts_packetizer_cfg_init_sized(moq_mpeg2ts_packetizer_cfg_t *c,
                                           size_t c_size)
{
    typedef moq_mpeg2ts_packetizer_cfg_t T;
    size_t n = sized_clear(c, c_size, sizeof(*c));
    if (COVERS(n, T, packet_size)) c->packet_size = MOQ_MPEG2TS_TS_PACKET_SIZE;
    if (COVERS(n, T, object_packets)) c->object_packets = 64;
    if (COVERS(n, T, group_at_random_access)) c->group_at_random_access = true;
    if (COVERS(n, T, random_access_pid))
        c->random_access_pid = MOQ_MPEG2TS_PID_AUTO;
    if (COVERS(n, T, wait_for_random_access)) c->wait_for_random_access = true;
    if (COVERS(n, T, pcr_pid)) c->pcr_pid = MOQ_MPEG2TS_PID_NONE;
}

moq_result_t moq_mpeg2ts_packetizer_init(moq_mpeg2ts_packetizer_t *pz,
                                         size_t pz_size,
                                         const moq_mpeg2ts_packetizer_cfg_t *c)
{
    if (!pz || !c || pz_size < sizeof(*pz)) return MOQ_ERR_INVAL;
    if (c->struct_size < PZ_CFG_V0) return MOQ_ERR_INVAL;
    if (!packet_size_ok(c->packet_size) || c->object_packets == 0 || !c->emit ||
        !c->object_buf)
        return MOQ_ERR_INVAL;
    if (c->object_packets > SIZE_MAX / c->packet_size ||
        c->object_cap < (size_t)c->object_packets * c->packet_size)
        return MOQ_ERR_INVAL;
    if (c->lead_cap && !c->lead_buf) return MOQ_ERR_INVAL;
    if (!pid_sel_ok(c->random_access_pid) ||
        (c->pcr_pid > MOQ_MPEG2TS_PID_MAX && c->pcr_pid != MOQ_MPEG2TS_PID_NONE))
        return MOQ_ERR_INVAL;
    memset(pz, 0, sizeof(*pz));
    pz->struct_size = sizeof(*pz);
    memcpy(&pz->_cfg, c, PZ_CFG_V0);
    pz->_cfg.struct_size = sizeof(pz->_cfg);
    /* The lead-in shares the first Object with the random access point. */
    size_t lead_packets = c->lead_cap / c->packet_size;
    if (lead_packets > (size_t)c->object_packets - 1)
        lead_packets = (size_t)c->object_packets - 1;
    pz->_lead_cap = lead_packets * c->packet_size;
    pz->_pcr_pid = c->pcr_pid;
    pz->_have_pcr_pid = c->pcr_pid != MOQ_MPEG2TS_PID_NONE;
    pz->_pmt_pid = MOQ_MPEG2TS_PID_NONE;
    pz->_video_pid = MOQ_MPEG2TS_PID_NONE;
    return MOQ_OK;
}

static void pz_emit_object(moq_mpeg2ts_packetizer_t *pz)
{
    if (pz->_obj_len == 0) return;
    moq_mpeg2ts_emitted_object_t o;
    memset(&o, 0, sizeof(o));
    o.payload.data = pz->_cfg.object_buf;
    o.payload.len = pz->_obj_len;
    o.group = pz->_group;
    o.object = pz->_object;
    o.new_group = pz->_object == 0;
    o.packet_count = pz->_obj_len / pz->_cfg.packet_size;
    o.random_access = pz->_obj_rap;
    pz->_cfg.emit(pz->_cfg.ctx, &o);
    pz->_stats.objects++;
    pz->_object++;
    pz->_obj_len = 0;
    pz->_obj_rap = false;
}

static void pz_append(moq_mpeg2ts_packetizer_t *pz, const uint8_t *pkt,
                      bool rap)
{
    size_t ps = pz->_cfg.packet_size;
    memcpy(pz->_cfg.object_buf + pz->_obj_len, pkt, ps);
    pz->_obj_len += ps;
    if (rap) pz->_obj_rap = true;
    if (pz->_obj_len >= (size_t)pz->_cfg.object_packets * ps)
        pz_emit_object(pz);
}

static void pz_flush_lead(moq_mpeg2ts_packetizer_t *pz)
{
    size_t ps = pz->_cfg.packet_size;
    if (pz->_group_open) {
        for (size_t o = 0; o < pz->_lead_len; o += ps)
            pz_append(pz, pz->_cfg.lead_buf + o, false);
    } else {
        pz->_stats.packets_dropped_before_random_access += pz->_lead_len / ps;
    }
    pz->_lead_len = 0;
    pz->_lead_active = false;
}

static void pz_close_group(moq_mpeg2ts_packetizer_t *pz)
{
    if (!pz->_group_open) return;
    pz_emit_object(pz);
    if (pz->_have_group_pcr) {
        uint64_t d = moq_mpeg2ts_pcr_delta(pz->_group_start_pcr,
                                           pz->_group_last_pcr);
        if (d > pz->_stats.max_group_27mhz) pz->_stats.max_group_27mhz = d;
        if (d > UINT64_C(2) * UINT64_C(27000000)) pz->_stats.groups_over_2s++;
    }
}

static void pz_open_group(moq_mpeg2ts_packetizer_t *pz)
{
    if (pz->_group_open) {
        pz_close_group(pz);
        pz->_group++;
    }
    pz->_group_open = true;
    pz->_object = 0;
    pz->_have_group_pcr = false;
    pz->_stats.groups++;
}

moq_result_t moq_mpeg2ts_packetizer_push(moq_mpeg2ts_packetizer_t *pz,
                                         moq_bytes_t packet)
{
    if (!pz || pz->struct_size != sizeof(*pz)) return MOQ_ERR_INVAL;
    uint32_t ps = pz->_cfg.packet_size;
    if (!packet.data || packet.len != ps) return MOQ_ERR_INVAL;
    moq_mpeg2ts_packet_t p;
    moq_result_t prc = moq_mpeg2ts_parse_packet(packet, ps, &p);
    if (prc != MOQ_OK) {
        /* Framing must hold; a damaged adaptation field is still carried. */
        if (packet.data[ts_offset(ps)] != MOQ_MPEG2TS_SYNC_BYTE)
            return MOQ_ERR_PROTO;
        moq_mpeg2ts_packet_init(&p);
        p.pid = pkt_pid(packet.data + ts_offset(ps));
    }
    pz->_stats.packets_in++;

    psi_view_t v = {
        .want_program = pz->_cfg.program_number,
        .pmt_pid = &pz->_pmt_pid,
        .have_pmt = &pz->_have_pmt,
        .video_pid = &pz->_video_pid,
        .pcr_pid = NULL,
        .sel_have = &pz->_sel_have,
        .sel_version = &pz->_sel_version,
        .sel_section = &pz->_sel_section,
        .sel_program = &pz->_sel_program,
        .pat_s = &pz->_pat_s,
        .pmt_s = &pz->_pmt_s,
        .pat_store = pz->_pat_store,
        .pmt_store = pz->_pmt_store,
    };
    psi_feed(&v, &p);

    /* PCR: rate between consecutive PCRs, Group duration. */
    if (p.has_pcr) {
        if (!pz->_have_pcr_pid) {
            pz->_have_pcr_pid = true;
            pz->_pcr_pid = p.pid;
        }
        if (p.pid == pz->_pcr_pid) {
            uint64_t val = moq_mpeg2ts_pcr_27mhz(p.pcr_base, p.pcr_ext);
            if (p.discontinuity) {
                if (pz->_group_open) pz->_stats.source_pcr_discontinuities++;
                pz->_have_last_pcr = false;
            } else if (pz->_have_last_pcr) {
                uint64_t d = moq_mpeg2ts_pcr_delta(pz->_last_pcr, val);
                if (d > 0) {
                    uint64_t bits = (pz->_bytes_since_pcr) * 8u;
                    uint64_t rate = muldiv_sat(bits, UINT64_C(27000000), d);
                    if (rate > pz->_stats.peak_rate_bps)
                        pz->_stats.peak_rate_bps = rate;
                }
            }
            if (!p.discontinuity) {
                pz->_have_last_pcr = true;
                pz->_last_pcr = val;
                pz->_cur_pcr = val;
                pz->_have_cur_pcr = true;
            }
            pz->_bytes_since_pcr = 0;
        }
    }
    if (pz->_have_last_pcr) pz->_bytes_since_pcr += MOQ_MPEG2TS_TS_PACKET_SIZE;

    uint16_t sel = ra_resolve(pz->_cfg.random_access_pid, pz->_have_pmt,
                              pz->_video_pid);
    bool rap = rap_step(pz->_armed, &p, sel);
    bool want_lead = pz->_lead_cap >= ps && pz->_cfg.group_at_random_access;

    if (!pz->_started) {
        pz->_started = true;
        if (!pz->_cfg.wait_for_random_access) pz_open_group(pz);
    }

    bool start_group = false;
    if (rap && (!pz->_group_open || pz->_cfg.group_at_random_access)) {
        if (!pz->_group_open) {
            start_group = true;
        } else if (pz->_have_group_pcr && pz->_have_cur_pcr) {
            uint64_t min = (uint64_t)pz->_cfg.min_group_ms * UINT64_C(27000);
            start_group = moq_mpeg2ts_pcr_delta(pz->_group_start_pcr,
                                                pz->_cur_pcr) >= min;
        } else {
            start_group = pz->_cfg.min_group_ms == 0;
        }
    }

    if (start_group) {
        if (pz->_group_open) {
            pz_emit_object(pz);   /* the open Object ends before the lead-in */
        }
        size_t lead_len = pz->_lead_len;
        pz->_lead_len = 0;
        pz->_lead_active = false;
        pz_open_group(pz);
        /* At most object_packets - 1 lead-in packets: the random access
         * point lands in the same, first, Object. */
        for (size_t o = 0; o < lead_len; o += ps)
            pz_append(pz, pz->_cfg.lead_buf + o, false);
        pz_append(pz, packet.data, true);
    } else if (rap && pz->_lead_active) {
        /* A random access point that opens no Group ends the lead-in. */
        pz_flush_lead(pz);
        if (pz->_group_open) pz_append(pz, packet.data, true);
        else pz->_stats.packets_dropped_before_random_access++;
    } else if (want_lead && p.pid == MOQ_MPEG2TS_PID_PAT && p.payload_unit_start) {
        if (pz->_lead_active) pz_flush_lead(pz);
        pz->_lead_active = true;
        memcpy(pz->_cfg.lead_buf, packet.data, ps);
        pz->_lead_len = ps;
    } else if (pz->_lead_active) {
        if (pz->_lead_len + ps > pz->_lead_cap) {
            pz_flush_lead(pz);
            if (pz->_group_open) pz_append(pz, packet.data, rap);
            else pz->_stats.packets_dropped_before_random_access++;
        } else {
            memcpy(pz->_cfg.lead_buf + pz->_lead_len, packet.data, ps);
            pz->_lead_len += ps;
        }
    } else if (pz->_group_open) {
        pz_append(pz, packet.data, rap);
    } else {
        pz->_stats.packets_dropped_before_random_access++;
    }

    /* Group PCR bounds, after the packet is placed. */
    if (pz->_group_open && p.has_pcr && p.pid == pz->_pcr_pid &&
        !p.discontinuity) {
        uint64_t val = moq_mpeg2ts_pcr_27mhz(p.pcr_base, p.pcr_ext);
        if (!pz->_have_group_pcr) {
            pz->_have_group_pcr = true;
            pz->_group_start_pcr = val;
        }
        pz->_group_last_pcr = val;
    }
    return MOQ_OK;
}

moq_result_t moq_mpeg2ts_packetizer_end_object(moq_mpeg2ts_packetizer_t *pz)
{
    if (!pz || pz->struct_size != sizeof(*pz)) return MOQ_ERR_INVAL;
    pz_emit_object(pz);
    return MOQ_OK;
}

moq_result_t moq_mpeg2ts_packetizer_flush(moq_mpeg2ts_packetizer_t *pz)
{
    if (!pz || pz->struct_size != sizeof(*pz)) return MOQ_ERR_INVAL;
    if (pz->_lead_active) pz_flush_lead(pz);
    pz_close_group(pz);
    return MOQ_OK;
}

moq_result_t moq_mpeg2ts_packetizer_get_stats(
    const moq_mpeg2ts_packetizer_t *pz,
    moq_mpeg2ts_packetizer_stats_t *out, size_t out_size)
{
    if (!pz || !out || pz->struct_size != sizeof(*pz) || out_size < PZ_STATS_V0)
        return MOQ_ERR_INVAL;
    moq_mpeg2ts_packetizer_stats_t snap = pz->_stats;
    size_t n = out_size < sizeof(snap) ? out_size : sizeof(snap);
    snap.struct_size = (uint32_t)n;
    memcpy(out, &snap, n);
    return MOQ_OK;
}
