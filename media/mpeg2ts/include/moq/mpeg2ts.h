#ifndef MOQ_MPEG2TS_H
#define MOQ_MPEG2TS_H

/*
 * MPEG-2 Transport Stream packaging helpers for the MSF "mpeg2ts" packaging
 * (draft-gregoire-moq-msfts, MSFTS format version 3).
 *
 * An mpeg2ts track carries whole TS source packets: 188-octet TS packets, or
 * 192-octet M2TS source packets (a four-octet prefix followed by a TS
 * packet). This library checks Object payloads against the packaging rules,
 * inspects the TS syntax that the packaging relies on (PAT, PMT, CAT, PCR,
 * PES timestamps, random access points), and gives a publisher and a
 * subscriber building blocks for what the draft asks of them. Obligations
 * that need knowledge of the whole application (which tracks to subscribe
 * to, when to publish a new catalog, how to schedule egress) stay with the
 * application; the functions below say which part they cover.
 *
 * It is a demultiplexer-lite, not a decoder: it never looks inside an
 * elementary stream, never descrambles, and never allocates. Every field read
 * from a packet is bounds-checked; input is treated as adversarial. The only
 * shared data is a constant CRC table, so independent state structs may be
 * used from different threads at once.
 *
 * Borrowing: moq_bytes_t outputs point into the caller's input (or into the
 * caller's storage) and are valid while that buffer is, and for the section
 * reassembler only until its next call.
 *
 * Struct layouts:
 *   - Caller-owned state (section reassembler, receiver, program filter,
 *     switcher, packetizer) is initialised by *_init(p, sizeof *p, ...),
 *     which refuses a struct smaller than this build's. Its fields are
 *     private, and its size and layout are not frozen: they may grow before
 *     release, so allocate it with sizeof from the header it is used with
 *     rather than a fixed byte count. None of these structs points into
 *     itself, so a byte copy may be moved to another address and used there
 *     in place of the original.
 *     A copy is not an independent duplicate where the state refers to
 *     caller storage: the section reassembler's storage, the receiver's
 *     reorder slots and the packetizer's Object and lead-in buffers stay
 *     shared, so only one of the copies may go on being used.
 *   - Inputs with a leading struct_size (object rules, inspection config,
 *     and the receiver, filter and packetizer configs) are stamped by
 *     *_init_sized(p, size), or by the *_init(p) macro, which passes
 *     sizeof *p. The initializer clears and stamps min(size, this build's
 *     size), sets a default only on a field the prefix covers whole, and
 *     writes nothing when size cannot hold struct_size. A call refuses a
 *     struct_size below the size the struct had when first published
 *     (its *_V0_SIZE) and reads a field added later only when struct_size
 *     covers it.
 *   - Outputs with a leading struct_size (object report, object info,
 *     receive result, delivery, switcher and packetizer stats) are filled
 *     through (out, out_size): the call refuses an out_size below the
 *     output's *_V0_SIZE before it changes any state, writes
 *     min(out_size, this build's size) octets, and stamps struct_size with
 *     the octets written. A delivery's embedded receive result is stamped
 *     with its own full size.
 *   - Every other struct is a plain record with a frozen layout. A later
 *     revision adds a new struct and a new function rather than growing one
 *     of these.
 *
 * Link against moq::mpeg2ts (depends only on moq::core).
 */

#include <stddef.h>
#include <stdint.h>

#include <moq/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -- Constants -------------------------------------------------------- */

#define MOQ_MPEG2TS_TS_PACKET_SIZE    188u
#define MOQ_MPEG2TS_M2TS_PACKET_SIZE  192u
#define MOQ_MPEG2TS_PREFIX_SIZE       4u
#define MOQ_MPEG2TS_SYNC_BYTE         0x47u

#define MOQ_MPEG2TS_PID_PAT           0x0000u
#define MOQ_MPEG2TS_PID_CAT           0x0001u
#define MOQ_MPEG2TS_PID_NULL          0x1FFFu
#define MOQ_MPEG2TS_PID_MAX           0x1FFFu
/* "No PID" for parameters that take an optional PID. Outside the 13-bit
 * PID space, so it never matches a packet. As a random access selector it
 * means "any PID". */
#define MOQ_MPEG2TS_PID_NONE          0xFFFFu
/* "The program's video PID", for the random access and timing selectors:
 * the first elementary stream of a video stream_type that the PMT of the
 * selected program lists. Until that PMT is known, a PES whose stream_id is
 * a video stream_id (0xE0..0xEF) qualifies; when the PMT lists no video
 * stream, any PID does. Also outside the PID space. */
#define MOQ_MPEG2TS_PID_AUTO          0xFFFEu

/* PTS, DTS and the PCR base run on a 90 kHz clock and wrap at 2^33. The full
 * PCR (base * 300 + extension) runs at 27 MHz and wraps at 2^33 * 300. */
#define MOQ_MPEG2TS_PTS_WRAP          (UINT64_C(1) << 33)
#define MOQ_MPEG2TS_PCR_WRAP          (MOQ_MPEG2TS_PTS_WRAP * UINT64_C(300))

/* Largest PSI section: section_length is at most 1021, plus the three octets
 * before it (ISO/IEC 13818-1, 2.4.4). */
#define MOQ_MPEG2TS_SECTION_MAX       1024u

/* -- Source packets ---------------------------------------------------- */

/* One parsed source packet (frozen layout). Header fields are always set;
 * adaptation-field fields are meaningful only when has_adaptation_field is
 * true. */
typedef struct moq_mpeg2ts_packet {
    bool        has_prefix;          /* 192-octet source packet */
    uint32_t    prefix;              /* big-endian four-octet prefix */

    uint16_t    pid;
    bool        transport_error;
    bool        payload_unit_start;
    bool        transport_priority;
    uint8_t     scrambling;          /* transport_scrambling_control */
    uint8_t     adaptation_field_control;
    uint8_t     continuity_counter;

    bool        has_adaptation_field;
    uint8_t     adaptation_field_length;
    bool        discontinuity;       /* discontinuity_indicator */
    bool        random_access;       /* random_access_indicator */
    bool        es_priority;
    bool        has_pcr;
    uint64_t    pcr_base;            /* 33 bits, 90 kHz */
    uint16_t    pcr_ext;             /* 0..299, 27 MHz remainder */

    moq_bytes_t payload;             /* BORROWED; empty when there is none */
} moq_mpeg2ts_packet_t;

MOQ_API void moq_mpeg2ts_packet_init(moq_mpeg2ts_packet_t *pkt);

/*
 * Parse one source packet of `packet_size` octets (188 or 192) from the start
 * of `src`.
 *
 * Returns MOQ_OK on success.
 * Returns MOQ_ERR_INVAL for a NULL argument, a packet_size other than 188 or
 *   192, or src shorter than packet_size.
 * Returns MOQ_ERR_PROTO when the sync byte is not 0x47, when
 *   adaptation_field_control is the reserved value 0, when the adaptation
 *   field overruns the packet or is too short for the optional fields its
 *   flags announce (PCR, OPCR, splice_countdown, transport private data,
 *   adaptation field extension), or when program_clock_reference_extension
 *   is 300 or more (ISO/IEC 13818-1, 2.4.3.5: it counts 0..299). Header
 *   fields parsed before the failure are left in *out.
 */
MOQ_API moq_result_t moq_mpeg2ts_parse_packet(moq_bytes_t src,
                                              uint32_t packet_size,
                                              moq_mpeg2ts_packet_t *out);

/* Clock arithmetic on the values a parsed packet exposes. Pure functions,
 * inline so that they need no library state. */

/* PCR in 27 MHz ticks: base * 300 + ext, reduced modulo the PCR wrap.
 * pcr_ext is 0..299 in a valid PCR (moq_mpeg2ts_parse_packet refuses larger
 * values); a larger one still yields a value inside the wrap. */
static inline uint64_t moq_mpeg2ts_pcr_27mhz(uint64_t pcr_base, uint16_t pcr_ext)
{
    return ((pcr_base % MOQ_MPEG2TS_PTS_WRAP) * 300u + (uint64_t)pcr_ext) %
           MOQ_MPEG2TS_PCR_WRAP;
}

/* Forward distance from `from` to `to` on the 27 MHz PCR clock, modulo the
 * PCR wrap. A wrap of the PCR base is an ordinary step, not a discontinuity. */
static inline uint64_t moq_mpeg2ts_pcr_delta(uint64_t from, uint64_t to)
{
    from %= MOQ_MPEG2TS_PCR_WRAP;
    to %= MOQ_MPEG2TS_PCR_WRAP;
    return to >= from ? to - from : MOQ_MPEG2TS_PCR_WRAP - from + to;
}

/* Forward distance between two 90 kHz timestamps, modulo 2^33. */
static inline uint64_t moq_mpeg2ts_pts_delta(uint64_t from, uint64_t to)
{
    return (to - from) & (MOQ_MPEG2TS_PTS_WRAP - 1u);
}

/* Arrival time and copy permission indicator of an "arrival-time" prefix
 * (BDAV convention: two bits of copy permission, thirty bits of 27 MHz
 * arrival time). */
static inline uint32_t moq_mpeg2ts_prefix_arrival_time(uint32_t prefix)
{
    return prefix & 0x3FFFFFFFu;
}

static inline uint8_t moq_mpeg2ts_prefix_copy_permission(uint32_t prefix)
{
    return (uint8_t)(prefix >> 30);
}

/* -- PES header timestamps -------------------------------------------- */

typedef struct moq_mpeg2ts_pes_header {   /* frozen layout */
    uint8_t  stream_id;
    bool     has_pts;
    uint64_t pts;                    /* 33 bits, 90 kHz */
    bool     has_dts;
    uint64_t dts;
} moq_mpeg2ts_pes_header_t;

/*
 * Read the stream_id, PTS and DTS of a PES packet whose first octets are in
 * `pes_start` (the payload of a packet with payload_unit_start set). Only the
 * header has to be present; the rest of the PES packet may follow in later
 * TS packets.
 *
 * Returns MOQ_OK on success, including for stream_ids that carry no optional
 * header (has_pts/has_dts false).
 * Returns MOQ_ERR_INVAL for a NULL argument.
 * Returns MOQ_ERR_PROTO when the start code prefix is absent, when the
 *   optional header overruns pes_start, when a non-zero PES_packet_length is
 *   too short to hold the optional header (ISO/IEC 13818-1, 2.4.3.7: the
 *   length counts the octets that follow it; 0 is the unbounded video
 *   form), when PTS_DTS_flags is the forbidden value 01, or when a
 *   timestamp's marker bits are wrong.
 */
MOQ_API moq_result_t moq_mpeg2ts_parse_pes_header(moq_bytes_t pes_start,
                                                  moq_mpeg2ts_pes_header_t *out);

/* -- PSI section reassembly ------------------------------------------ */

/* Reassembly state shared by the stateful helpers. Private fields. `_in`
 * points into the packet most recently pushed and is used only until the
 * sections of that packet have been read. */
typedef struct moq_mpeg2ts_section_state {
    size_t         _len;             /* octets collected */
    size_t         _need;            /* full section length, 0 until known */
    uint16_t       _pid;
    bool           _active;          /* a section is being collected */
    bool           _have_last;       /* last payload-bearing packet seen */
    uint16_t       _last_pid;
    uint8_t        _last_cc;
    uint8_t        _last_len;
    bool           _last_pusi;       /* its payload_unit_start_indicator */
    uint8_t        _last_payload[184];  /* that packet's payload, to compare */
    const uint8_t *_in;
    size_t         _in_len;
    size_t         _pos;
    size_t         _cont_end;        /* end of bytes continuing a section */
    bool           _pusi;
    bool           _started;         /* new sections of this packet begun */
} moq_mpeg2ts_section_state_t;

/*
 * Reassembles PSI sections (ISO/IEC 13818-1, 2.4.4) from the packets of one
 * PID into caller storage. A packet can complete one section and start
 * others, and can carry several whole sections; every one of them is
 * returned. A section is assembled across packets only when each
 * continuation packet carries the next continuity counter. A duplicate
 * packet (the same continuity counter and payload as the previous packet,
 * 2.4.3.3) is ignored; a packet that repeats the counter with a different
 * payload, or skips one, abandons the partial section. A section longer than
 * the storage, or whose section_length exceeds 1021, is skipped.
 */
typedef struct moq_mpeg2ts_section_buf {
    uint32_t                    struct_size;
    uint8_t                    *_storage;
    size_t                      _cap;
    moq_mpeg2ts_section_state_t _s;
} moq_mpeg2ts_section_buf_t;

/* storage holds the sections that span packets; cap should be
 * MOQ_MPEG2TS_SECTION_MAX. Returns MOQ_ERR_INVAL for a NULL argument, a
 * sb_size smaller than this build's struct, or cap below 3. */
MOQ_API moq_result_t moq_mpeg2ts_section_buf_init(moq_mpeg2ts_section_buf_t *sb,
                                                  size_t sb_size,
                                                  uint8_t *storage, size_t cap);

/*
 * Feed one parsed packet. Then call moq_mpeg2ts_section_buf_next until it
 * returns MOQ_DONE to read the sections it completed. The packet's bytes must
 * stay valid until then.
 *
 * Returns MOQ_OK, MOQ_ERR_INVAL for a NULL argument or an uninitialised
 * struct, and MOQ_ERR_PROTO for a pointer_field that overruns the packet (the
 * packet yields no section and a partial one is abandoned).
 */
MOQ_API moq_result_t moq_mpeg2ts_section_buf_push(moq_mpeg2ts_section_buf_t *sb,
                                                  const moq_mpeg2ts_packet_t *pkt);

/* The next complete section of the packet last pushed: MOQ_OK with *section
 * set, MOQ_DONE when there is none left, MOQ_ERR_INVAL for a NULL argument or
 * an uninitialised struct. *section borrows from that packet or from the
 * storage, until the next call to any section_buf function. */
MOQ_API moq_result_t moq_mpeg2ts_section_buf_next(moq_mpeg2ts_section_buf_t *sb,
                                                  moq_bytes_t *section);

/* Drop any partial section and the duplicate-detection memory. */
MOQ_API void moq_mpeg2ts_section_buf_reset(moq_mpeg2ts_section_buf_t *sb);

/* -- PSI tables -------------------------------------------------------- */

typedef struct moq_mpeg2ts_program {      /* frozen layout */
    uint16_t program_number;         /* never 0 here: see network_pid */
    uint16_t pmt_pid;
} moq_mpeg2ts_program_t;

typedef struct moq_mpeg2ts_pat {          /* frozen layout */
    uint16_t               transport_stream_id;
    uint8_t                version;
    bool                   current_next;
    uint8_t                section_number;
    uint8_t                last_section_number;
    bool                   has_network_pid;   /* program_number 0 entry */
    uint16_t               network_pid;
    moq_mpeg2ts_program_t *programs;          /* caller storage */
    size_t                 program_count;
    size_t                 program_cap;
} moq_mpeg2ts_pat_t;

MOQ_API void moq_mpeg2ts_pat_init(moq_mpeg2ts_pat_t *pat,
                                  moq_mpeg2ts_program_t *programs, size_t cap);

/*
 * Parse one complete PAT section (from table_id through CRC_32). A PAT can be
 * split over several sections (section_number .. last_section_number); this
 * reads one of them.
 *
 * Returns MOQ_OK on success.
 * Returns MOQ_ERR_INVAL for a NULL argument.
 * Returns MOQ_ERR_PROTO for a wrong table_id, section_syntax_indicator 0, a
 *   section_length that disagrees with the input or exceeds 1021, a
 *   section_number above last_section_number, a program loop that is not a
 *   whole number of entries, or a bad CRC_32.
 * Returns MOQ_ERR_BUFFER when the section lists more programs than
 *   program_cap; program_count is then the number required and the first
 *   program_cap entries are filled.
 */
MOQ_API moq_result_t moq_mpeg2ts_parse_pat(moq_bytes_t section,
                                           moq_mpeg2ts_pat_t *out);

typedef struct moq_mpeg2ts_stream {       /* frozen layout */
    uint8_t     stream_type;
    uint16_t    pid;
    moq_bytes_t descriptors;         /* BORROWED from the section */
} moq_mpeg2ts_stream_t;

typedef struct moq_mpeg2ts_pmt {          /* frozen layout */
    uint16_t              program_number;
    uint8_t               version;
    bool                  current_next;
    uint16_t              pcr_pid;
    moq_bytes_t           program_descriptors;   /* BORROWED */
    moq_mpeg2ts_stream_t *streams;               /* caller storage */
    size_t                stream_count;
    size_t                stream_cap;
} moq_mpeg2ts_pmt_t;

MOQ_API void moq_mpeg2ts_pmt_init(moq_mpeg2ts_pmt_t *pmt,
                                  moq_mpeg2ts_stream_t *streams, size_t cap);

/*
 * Parse a complete PMT section. Error contract as moq_mpeg2ts_parse_pat, with
 * MOQ_ERR_PROTO also for a section_number or last_section_number other than
 * 0 (2.4.4.9), and for a descriptor loop, or a descriptor inside one, that
 * overruns its bounds. On MOQ_ERR_BUFFER the first stream_cap streams are
 * filled.
 */
MOQ_API moq_result_t moq_mpeg2ts_parse_pmt(moq_bytes_t section,
                                           moq_mpeg2ts_pmt_t *out);

/* True for the stream_type values of video elementary streams (ISO/IEC
 * 13818-1 Table 2-34 and its amendments: MPEG-1/2 video, MPEG-4 Visual, AVC
 * and its SVC/MVC extensions, HEVC, VVC, and the like). */
MOQ_API bool moq_mpeg2ts_stream_type_is_video(uint8_t stream_type);

/*
 * Rewrite a PMT section to keep only the elementary streams whose PID is in
 * keep_pids, increment version_number (modulo 32) and recompute CRC_32 -- the
 * table rewrite that the ES-Packets section requires of a subscriber that
 * carries a subset of the streams a PMT lists. Program descriptors are kept.
 * Which streams to carry, and emitting the result before the elementary
 * stream packets and at the required repetition interval, are the
 * subscriber's.
 * Returns MOQ_ERR_PROTO for a malformed input section, MOQ_ERR_BUFFER (with
 * *out_len) when out is too small, MOQ_ERR_INVAL for NULL arguments.
 */
MOQ_API moq_result_t moq_mpeg2ts_pmt_subset(moq_bytes_t pmt_section,
                                            const uint16_t *keep_pids,
                                            size_t keep_count,
                                            uint8_t *out, size_t cap,
                                            size_t *out_len);

/*
 * Carry a PSI section in TS packets on `pid`: pointer_field 0 in the first
 * packet, 0xFF stuffing after the section. *cc is the continuity counter of
 * the first packet and is advanced past the last. packet_size 192 writes a
 * zero prefix. Returns MOQ_ERR_BUFFER (with *out_len) when out is too small.
 */
MOQ_API moq_result_t moq_mpeg2ts_packetize_section(uint16_t pid, uint8_t *cc,
                                                   moq_bytes_t section,
                                                   uint32_t packet_size,
                                                   uint8_t *out, size_t cap,
                                                   size_t *out_len);

/* -- Object payload validation (Object Payload Format) ---------------- */

typedef enum moq_mpeg2ts_validity {
    MOQ_MPEG2TS_OK                 = 0,
    MOQ_MPEG2TS_ERR_EMPTY          = 1, /* no source packet at all */
    MOQ_MPEG2TS_ERR_PARTIAL_PACKET = 2, /* length not a multiple of the size */
    MOQ_MPEG2TS_ERR_SYNC_BYTE      = 3, /* 0x47 missing at offset 0 / 4 */
    MOQ_MPEG2TS_ERR_NULL_PACKET    = 4, /* es-packets: a null packet */
    MOQ_MPEG2TS_ERR_FOREIGN_PID    = 5, /* es-packets: a packet off es_pid */
} moq_mpeg2ts_validity_t;

typedef struct moq_mpeg2ts_object_rules {
    uint32_t struct_size;
    uint32_t packet_size;            /* 188 or 192 */
    bool     es_packets;             /* mpeg2tsMode "es-packets" */
    uint16_t es_pid;                 /* mpeg2tsEsPid, when es_packets */
} moq_mpeg2ts_object_rules_t;

#define MOQ_MPEG2TS_OBJECT_RULES_V0_SIZE \
    (offsetof(moq_mpeg2ts_object_rules_t, es_pid) + sizeof(uint16_t))

/* packet_size 188, not es-packets. */
MOQ_API void moq_mpeg2ts_object_rules_init_sized(
    moq_mpeg2ts_object_rules_t *rules, size_t rules_size);
#define moq_mpeg2ts_object_rules_init(rules) \
    moq_mpeg2ts_object_rules_init_sized((rules), sizeof(*(rules)))

typedef struct moq_mpeg2ts_object_report {
    uint32_t               struct_size;
    moq_mpeg2ts_validity_t reason;        /* MOQ_MPEG2TS_OK when valid */
    size_t                 packet_count;  /* whole source packets */
    size_t                 bad_packet;    /* index of the first failing packet */
} moq_mpeg2ts_object_report_t;

#define MOQ_MPEG2TS_OBJECT_REPORT_V0_SIZE \
    (offsetof(moq_mpeg2ts_object_report_t, bad_packet) + sizeof(size_t))

MOQ_API void moq_mpeg2ts_object_report_init_sized(
    moq_mpeg2ts_object_report_t *report, size_t report_size);
#define moq_mpeg2ts_object_report_init(report) \
    moq_mpeg2ts_object_report_init_sized((report), sizeof(*(report)))

/*
 * Check one Object payload:
 *   - it holds only whole source packets (Object Payload Format: a
 *     subscriber MUST reject an Object that breaks this rule), and at least
 *     one: an empty payload is refused as a local policy, since the draft
 *     sets no lower bound;
 *   - every source packet carries the sync byte 0x47 at offset 0 (188) or 4
 *     (192), as ISO/IEC 13818-1 requires;
 *   - in es-packets mode, every packet is on es_pid and none is a null packet
 *     (ES-Packets: the track MUST NOT contain null packets; ES PID: the track
 *     carries only the packets of that PID).
 * Nothing else about the TS syntax is judged here.
 *
 * The report is written as an output (see the top of this header).
 *
 * Returns MOQ_OK when valid, MOQ_ERR_PROTO when not (report->reason and
 * report->bad_packet say why and where), MOQ_ERR_INVAL for NULL arguments,
 * a rules struct_size below MOQ_MPEG2TS_OBJECT_RULES_V0_SIZE, a report_size
 * below MOQ_MPEG2TS_OBJECT_REPORT_V0_SIZE (nothing is written), a
 * packet_size other than 188/192, or an es_pid above 0x1FFF.
 */
MOQ_API moq_result_t moq_mpeg2ts_validate_object(
    const moq_mpeg2ts_object_rules_t *rules,
    moq_bytes_t payload,
    moq_mpeg2ts_object_report_t *report, size_t report_size);

/* -- Object inspection -------------------------------------------------- */

typedef struct moq_mpeg2ts_inspect_cfg {
    uint32_t           struct_size;
    uint32_t           packet_size;      /* 188 or 192 */
    /* PID of the PMT to read. PID_NONE: take it from a PAT in the object. */
    uint16_t           pmt_pid;
    /* Program whose PMT to read. 0: the first program of the PAT. */
    uint16_t           program_number;
    /* PID whose PES gives the timing. PID_NONE: the PES at the first random
     * access point, else the first PES that carries a PTS. PID_AUTO: the
     * program's video PID (see MOQ_MPEG2TS_PID_AUTO). */
    uint16_t           timing_pid;
    /* PID whose PCR is reported. PID_NONE: the PMT's PCR_PID when known,
     * else the first PCR in the object. */
    uint16_t           pcr_pid;
    /* PID whose random access points count. PID_NONE: any PID. PID_AUTO:
     * the program's video PID. */
    uint16_t           random_access_pid;
    /* Optional outputs (NULL to skip). The PMT is parsed straight into
     * *pmt's own table; its descriptor spans, and those of every stream
     * entry written, are left empty because the section is reassembled in
     * scratch that does not outlive the call. */
    moq_mpeg2ts_pat_t *pat;
    moq_mpeg2ts_pmt_t *pmt;
} moq_mpeg2ts_inspect_cfg_t;

#define MOQ_MPEG2TS_INSPECT_CFG_V0_SIZE \
    (offsetof(moq_mpeg2ts_inspect_cfg_t, pmt) + \
     sizeof(moq_mpeg2ts_pmt_t *))

/* packet_size 188, every PID selector PID_NONE, program_number 0. */
MOQ_API void moq_mpeg2ts_inspect_cfg_init_sized(
    moq_mpeg2ts_inspect_cfg_t *cfg, size_t cfg_size);
#define moq_mpeg2ts_inspect_cfg_init(cfg) \
    moq_mpeg2ts_inspect_cfg_init_sized((cfg), sizeof(*(cfg)))

typedef struct moq_mpeg2ts_object_info {
    uint32_t struct_size;
    size_t   packet_count;
    size_t   null_packets;
    size_t   malformed_packets;   /* adaptation field or PES header rejected */
    size_t   scrambled_packets;   /* transport_scrambling_control != 0 */

    /* PSI. psi_complete: a PAT and the PMT of the selected program, both
     * with a valid CRC_32, were seen in this object. */
    bool     has_pat;
    bool     has_pmt;
    bool     psi_complete;
    size_t   bad_crc_sections;
    uint16_t program_number;      /* program the PMT belongs to */
    uint16_t pmt_pid;
    uint16_t pmt_pcr_pid;         /* PCR_PID from the PMT */
    uint16_t pmt_video_pid;       /* first video stream of the PMT, or NONE */

    /* Random access point: random_access_indicator on a packet of the
     * selected PID, at the start of a PES packet. The indicator may sit on
     * that packet or on an earlier packet of the same PID in the object
     * (2.4.3.5: it announces the next PES packet to start). */
    bool     has_random_access;
    size_t   random_access_index; /* the packet that starts that PES */
    uint16_t random_access_pid;

    /* Timing from the selected PES (see timing_pid). */
    bool     has_pts;
    uint64_t pts;
    bool     has_dts;
    uint64_t dts;
    uint16_t timing_pid;          /* MOQ_MPEG2TS_PID_NONE when no PES */

    /* PCR on the reported PCR PID. */
    uint16_t pcr_pid;             /* MOQ_MPEG2TS_PID_NONE when none */
    size_t   pcr_count;
    bool     has_pcr;
    uint64_t first_pcr;           /* 27 MHz */
    size_t   first_pcr_index;
    uint64_t last_pcr;
    size_t   last_pcr_index;
    /* discontinuity_indicator set on a PCR-bearing packet of pcr_pid. */
    bool     pcr_discontinuity;
    size_t   pcr_discontinuity_index;
} moq_mpeg2ts_object_info_t;

#define MOQ_MPEG2TS_OBJECT_INFO_V0_SIZE \
    (offsetof(moq_mpeg2ts_object_info_t, pcr_discontinuity_index) + \
     sizeof(size_t))

/* Every PID field PID_NONE. */
MOQ_API void moq_mpeg2ts_object_info_init_sized(
    moq_mpeg2ts_object_info_t *info, size_t info_size);
#define moq_mpeg2ts_object_info_init(info) \
    moq_mpeg2ts_object_info_init_sized((info), sizeof(*(info)))

/*
 * Inspect the source packets of one Object payload. The payload must already
 * be valid framing (see moq_mpeg2ts_validate_object); a packet whose header
 * fields cannot be read is counted in malformed_packets and skipped, never
 * trusted. Sections packed in one packet and sections spanning packets of
 * the object are all read.
 *
 * The info is written as an output (see the top of this header).
 *
 * Returns MOQ_OK, MOQ_ERR_INVAL for NULL arguments, a cfg struct_size
 * below MOQ_MPEG2TS_INSPECT_CFG_V0_SIZE, an info_size below
 * MOQ_MPEG2TS_OBJECT_INFO_V0_SIZE (nothing is written, cfg->pat and
 * cfg->pmt included), or framing that is not a whole number of
 * packet_size packets, and MOQ_ERR_BUFFER when cfg->pat or cfg->pmt is
 * too small (the inspection is still complete; the count in that struct is
 * the number needed and its table holds the entries that fit).
 */
MOQ_API moq_result_t moq_mpeg2ts_inspect_object(
    const moq_mpeg2ts_inspect_cfg_t *cfg,
    moq_bytes_t payload,
    moq_mpeg2ts_object_info_t *info, size_t info_size);

/* -- Subscriber: reconstructing the packet stream ----------------------- */

/*
 * What the subscription tells about an Object's place in its Group. MoQT
 * drafts 16 and 18 carry no per-Object "last in Group" mark on a data
 * stream: the END_OF_GROUP bit is in the subgroup header, so it is set on
 * every Object of that subgroup, and the Group's final Object is the last
 * one before the subgroup stream's FIN (or an END_OF_GROUP status Object).
 */
/* The Object came on a subgroup whose header had the END_OF_GROUP bit:
 * that subgroup holds the largest Object of the Group. Recorded and handed
 * back with the Object; it does not by itself end the Group, which takes
 * the subgroup's FIN (moq_mpeg2ts_receiver_end_group). */
#define MOQ_MPEG2TS_OBJ_SUBGROUP_ENDS_GROUP (1u << 0)
/* The Object is known to be the final Object of its Group: its subgroup
 * (with END_OF_GROUP) ended with a FIN after it, or an END_OF_GROUP status
 * followed it. */
#define MOQ_MPEG2TS_OBJ_LAST_IN_GROUP       (1u << 1)
/* The receiver has not released anything yet, and the caller knows its
 * first Objects come from a join in the middle of a Group (for example a
 * passive subscription whose Forward State was just raised): such an Object
 * may start the receiver once it has waited MOQ_MPEG2TS_JOIN_GRACE_US (at
 * most the reorder window), without waiting for Object 0 or the whole
 * window. The grace still lets a slightly reordered first pair go out in
 * order. Ignored once the receiver has started. The grace is fixed: the
 * receiver does not know the Object interval, and 50 ms is about two
 * Objects at the rates MPEG-TS tracks use (one Object every 20-30 ms); a
 * window shorter than the grace bounds it. */
#define MOQ_MPEG2TS_OBJ_JOINABLE            (1u << 2)
#define MOQ_MPEG2TS_JOIN_GRACE_US           50000u

/* One held Object of the receiver's reorder window (frozen layout). The
 * caller provides an array of these and never touches its contents. */
typedef struct moq_mpeg2ts_reorder_slot {
    bool        used;
    uint32_t    flags;
    uint64_t    group;
    uint64_t    object;
    uint64_t    arrival_us;
    moq_bytes_t payload;
    void       *ctx;
} moq_mpeg2ts_reorder_slot_t;

typedef struct moq_mpeg2ts_receiver_cfg {
    uint32_t struct_size;
    uint32_t packet_size;
    bool     es_packets;
    uint16_t es_pid;
    /* PID whose PCR measures gaps. PID_NONE: the PMT's PCR_PID once known,
     * else the first PCR PID seen. */
    uint16_t pcr_pid;
    /* PID whose random access points end a discontinuity. Default
     * PID_AUTO, the program's video PID: muxers commonly set
     * random_access_indicator on every audio frame, which is no place to
     * resume video. PID_NONE: any PID. */
    uint16_t random_access_pid;
    /* Program whose PMT gives the video and PCR PIDs. 0: the first program
     * of the PAT (Unmodified Multiplex: the reference program when the
     * catalog names none). */
    uint16_t program_number;
    /* Reorder window for moq_mpeg2ts_receiver_offer/_poll: caller storage
     * of reorder_slot_count slots (0 for none: moq_mpeg2ts_receiver_push
     * only), and how long an Object may wait for those before it in the
     * Location order. */
    moq_mpeg2ts_reorder_slot_t *reorder_slots;
    size_t   reorder_slot_count;
    uint64_t reorder_window_us;
} moq_mpeg2ts_receiver_cfg_t;

#define MOQ_MPEG2TS_RECEIVER_CFG_V0_SIZE \
    (offsetof(moq_mpeg2ts_receiver_cfg_t, reorder_window_us) + \
     sizeof(uint64_t))

/* 188-octet packets, PCR PID from the PMT, random access on the video PID,
 * the first program, no reorder window. */
MOQ_API void moq_mpeg2ts_receiver_cfg_init_sized(
    moq_mpeg2ts_receiver_cfg_t *cfg, size_t cfg_size);
#define moq_mpeg2ts_receiver_cfg_init(cfg) \
    moq_mpeg2ts_receiver_cfg_init_sized((cfg), sizeof(*(cfg)))

#define MOQ_MPEG2TS_RECEIVER_MAX_ENDS 8u

/* Caller-owned subscriber state. Private fields. Its size is dominated by
 * the per-PID random access bitmap and the two PSI section stores. */
typedef struct moq_mpeg2ts_receiver {
    uint32_t struct_size;
    moq_mpeg2ts_receiver_cfg_t _cfg;
    /* Sequencing. */
    bool     _started;
    uint64_t _group;
    uint64_t _object;
    bool     _finishing;
    struct {
        bool     used;
        uint64_t group;
        uint64_t final_object;
    }        _ends[MOQ_MPEG2TS_RECEIVER_MAX_ENDS];
    size_t   _ends_next;
    /* Gaps: a run of missing or rejected Objects. */
    bool     _run_open;
    bool     _run_reported;
    uint64_t _run_group;
    uint64_t _run_object;
    bool     _run_has_pcr;
    uint64_t _run_pcr;               /* last PCR before the run */
    bool     _measure_pending;       /* a closed run awaits its next PCR */
    bool     _measure_has_pcr;
    uint64_t _measure_pcr;
    bool     _discontinuous;
    /* Clock and random access. */
    bool     _has_pcr;
    uint64_t _last_pcr;
    uint16_t _pcr_pid;
    /* PIDs whose random_access_indicator announced the next PES. */
    uint8_t  _armed[(MOQ_MPEG2TS_PID_MAX + 1u) / 8u];
    /* PSI of the reconstructed stream. The reference program is taken from
     * the lowest-numbered section of the current PAT version that lists
     * one; only that program's PMT applies. */
    bool     _sel_have;
    uint8_t  _sel_version;
    uint8_t  _sel_section;
    uint16_t _sel_program;
    uint16_t _pmt_pid;
    bool     _have_pmt;
    uint16_t _video_pid;
    uint16_t _pmt_pcr_pid;
    moq_mpeg2ts_section_state_t _pat_s;
    moq_mpeg2ts_section_state_t _pmt_s;
    uint8_t  _pat_store[MOQ_MPEG2TS_SECTION_MAX];
    uint8_t  _pmt_store[MOQ_MPEG2TS_SECTION_MAX];
} moq_mpeg2ts_receiver_t;

/* Returns MOQ_ERR_INVAL when rx_size is smaller than this build's
 * moq_mpeg2ts_receiver_t, the config's struct_size is below its first
 * published size, or the config is invalid. */
MOQ_API moq_result_t moq_mpeg2ts_receiver_init(
    moq_mpeg2ts_receiver_t *rx, size_t rx_size,
    const moq_mpeg2ts_receiver_cfg_t *cfg);

typedef struct moq_mpeg2ts_receive_result {
    uint32_t               struct_size;
    /* The payload is valid and in order: append it to the reconstructed
     * stream unchanged. */
    bool                   accepted;
    moq_mpeg2ts_validity_t reason;        /* when the payload is invalid */
    bool                   out_of_order;  /* Location not after the last one */
    bool                   joined_mid_group; /* first Object is not Object 0 */

    /* A run of missing or rejected Objects began before this one, reported
     * once: gap_group/gap_object is the Location of its first Object. */
    bool                   gap;
    uint64_t               gap_group;
    uint64_t               gap_object;
    /* The run is only the possible tail of the previous Group: this Object
     * opens the next Group, and the previous Group's final Object was never
     * confirmed (no FIN on its END_OF_GROUP subgroup, no status). Its
     * Objects may all have arrived, or its tail may be lost. Such a run does
     * not make the stream discontinuous. A known loss (a rejected or skipped
     * Object) is never reported this way. */
    bool                   gap_tail_unknown;
    /* PCR span of the run, reported on the first accepted Object with a
     * PCR after it (from the last PCR before the run). */
    bool                   gap_duration_known;
    uint64_t               gap_duration_27mhz;

    /* Informational recovery state: a gap is behind the reconstructed
     * stream and no random access point on the selected PID has followed
     * yet. The draft does not ask a subscriber to hold delivery for it. */
    bool                   discontinuous;
    bool                   random_access;  /* this Object holds one */
} moq_mpeg2ts_receive_result_t;

#define MOQ_MPEG2TS_RECEIVE_RESULT_V0_SIZE \
    (offsetof(moq_mpeg2ts_receive_result_t, random_access) + sizeof(bool))

MOQ_API void moq_mpeg2ts_receive_result_init_sized(
    moq_mpeg2ts_receive_result_t *result, size_t result_size);
#define moq_mpeg2ts_receive_result_init(result) \
    moq_mpeg2ts_receive_result_init_sized((result), sizeof(*(result)))

/*
 * Account for one received Object at Location (group, object), in ascending
 * Location order. The payload is never modified: continuity counters are
 * left exactly as received (Object Payload Format: a subscriber MUST NOT
 * change the continuity counters to hide a gap). A rejected Object counts as
 * missing. `flags` is a set of MOQ_MPEG2TS_OBJ_* bits.
 *
 * Object IDs within a Group are taken to be dense (MSF: Object IDs start at
 * 0 and advance by one), so the expected next Location is (group, object +
 * 1), or (group + 1, 0) once the Group's final Object is known (from
 * MOQ_MPEG2TS_OBJ_LAST_IN_GROUP or moq_mpeg2ts_receiver_end_group). Moving
 * to (group + 1, 0) without that knowledge is a gap with gap_tail_unknown,
 * END_OF_GROUP bit or not: the bit names the subgroup holding the largest
 * Object, and only that subgroup's FIN says which Object it is (drafts 16
 * and 18; a reset stream says nothing). Any other jump is a gap. Objects that arrive out of order are rejected
 * with out_of_order; use the reorder window (moq_mpeg2ts_receiver_offer)
 * when the subscription delivers Groups on separate streams.
 *
 * The result is written as an output (see the top of this header).
 *
 * Returns MOQ_OK when the Object was accepted, MOQ_ERR_PROTO when it was
 * rejected (invalid payload or out of order), MOQ_ERR_INVAL for NULL
 * arguments, an uninitialised receiver, or a result_size below
 * MOQ_MPEG2TS_RECEIVE_RESULT_V0_SIZE (the Object is then not accounted:
 * the receiver is unchanged and nothing is written).
 */
MOQ_API moq_result_t moq_mpeg2ts_receiver_push(
    moq_mpeg2ts_receiver_t *rx,
    uint64_t group, uint64_t object, uint32_t flags,
    moq_bytes_t payload,
    moq_mpeg2ts_receive_result_t *result, size_t result_size);

/*
 * The Group's final Object is now known to be the largest Object of `group`
 * received so far: call it when a subgroup with the END_OF_GROUP bit ends
 * with a FIN (the session's MOQ_EVENT_SUBGROUP_FINISHED, which follows the
 * subgroup's last Object), after all of that subgroup's Objects were pushed
 * or offered. Lets the next Group start without a gap and without waiting
 * for the reorder window. Ignored when no Object of the Group was seen, and
 * never moves an end already known (for instance from
 * moq_mpeg2ts_receiver_end_group_at) back to an earlier Object.
 */
MOQ_API void moq_mpeg2ts_receiver_end_group(moq_mpeg2ts_receiver_t *rx,
                                            uint64_t group);

/*
 * The Group ends before `end_object`: call it for an END_OF_GROUP status
 * Object at (group, end_object). The status Object's ID is the first Object
 * ID that does not exist (drafts 16 and 18), so the final Object is
 * end_object - 1 and end_object 0 marks the Group empty. Unlike
 * moq_mpeg2ts_receiver_end_group this does not infer the final Object from
 * what arrived: a final Object that is lost or still in flight remains a gap
 * (or is still awaited by the reorder window), and an empty Group is stepped
 * over without a gap.
 */
MOQ_API void moq_mpeg2ts_receiver_end_group_at(moq_mpeg2ts_receiver_t *rx,
                                               uint64_t group,
                                               uint64_t end_object);

/*
 * Reorder window. Objects of different Groups ride different subgroup
 * streams and can arrive out of Location order. offer() holds an Object
 * (the caller keeps its payload alive until poll() hands it back); poll()
 * releases held Objects in Location order, each accounted as by
 * moq_mpeg2ts_receiver_push. The first held Object in Location order is
 * released when it is the expected next Location, when any held Object has
 * waited reorder_window_us (the longest-waiting one keeps the earlier
 * Locations from waiting longer), when every slot is in use, or after
 * moq_mpeg2ts_receiver_finish. A release that does not follow a confirmed
 * Group end reports the possible tail as gap_tail_unknown. At a Group
 * boundary whose end is not known, the next Group therefore waits up to the
 * window: call moq_mpeg2ts_receiver_end_group on the subgroup's FIN, or
 * offer the final Object with MOQ_MPEG2TS_OBJ_LAST_IN_GROUP, to release it
 * at once.
 */
typedef struct moq_mpeg2ts_delivery {
    uint32_t                     struct_size; /* stamped by poll() */
    uint64_t                     group;
    uint64_t                     object;
    uint32_t                     flags;
    moq_bytes_t                  payload;     /* as offered */
    void                        *ctx;         /* as offered */
    moq_mpeg2ts_receive_result_t result;
} moq_mpeg2ts_delivery_t;

#define MOQ_MPEG2TS_DELIVERY_V0_SIZE \
    (offsetof(moq_mpeg2ts_delivery_t, result) + \
     sizeof(moq_mpeg2ts_receive_result_t))

MOQ_API void moq_mpeg2ts_delivery_init_sized(moq_mpeg2ts_delivery_t *out,
                                             size_t out_size);
#define moq_mpeg2ts_delivery_init(out) \
    moq_mpeg2ts_delivery_init_sized((out), sizeof(*(out)))

/*
 * Returns MOQ_OK when the Object is held. MOQ_ERR_PROTO when its Location is
 * at or before the last Object already released, or equals one already
 * held: the Object is late or duplicated and is not held. MOQ_ERR_BUFFER
 * when every slot is in use: poll first. MOQ_ERR_INVAL for NULL arguments
 * or a receiver without slots.
 */
MOQ_API moq_result_t moq_mpeg2ts_receiver_offer(
    moq_mpeg2ts_receiver_t *rx, uint64_t now_us,
    uint64_t group, uint64_t object, uint32_t flags,
    moq_bytes_t payload, void *ctx);

/* Release the next Object that is due: MOQ_OK with *out set (written as an
 * output, see the top of this header), MOQ_DONE when nothing is due yet,
 * MOQ_ERR_INVAL for NULL arguments, an uninitialised receiver, or an
 * out_size below MOQ_MPEG2TS_DELIVERY_V0_SIZE (nothing is released: the
 * held Object, its payload and ctx, the Location and the deadline stay as
 * they were). */
MOQ_API moq_result_t moq_mpeg2ts_receiver_poll(moq_mpeg2ts_receiver_t *rx,
                                               uint64_t now_us,
                                               moq_mpeg2ts_delivery_t *out,
                                               size_t out_size);

/* The time from which poll() releases an Object for the window: the
 * earliest arrival of a held Object plus reorder_window_us, saturating at
 * UINT64_MAX; UINT64_MAX also when nothing is held. poll() at that time or
 * later makes progress. An Object may become due earlier, after an offer or
 * an end_group. */
MOQ_API uint64_t moq_mpeg2ts_receiver_next_deadline_us(
    const moq_mpeg2ts_receiver_t *rx);

/* End of the track: every held Object is due, in order. */
MOQ_API void moq_mpeg2ts_receiver_finish(moq_mpeg2ts_receiver_t *rx);

/*
 * Remove the four-octet prefix of each 192-octet source packet, for a
 * receiver that expects 188-octet TS packets (Timestamp Mode: such a
 * subscriber MUST remove the prefix; the caller decides when that applies).
 * `in` must be whole 192-octet packets; out must hold len / 192 * 188
 * octets. out may equal in.data (in place).
 */
MOQ_API moq_result_t moq_mpeg2ts_strip_prefix(moq_bytes_t in,
                                              uint8_t *out, size_t cap,
                                              size_t *out_len);

/*
 * Prepare PAT/PMT packets taken from the catalog (Use of MSF Initialization
 * Data) for a join: renumber the continuity counters of each PID's init
 * packets so that the first packet on that PID in `first_payload` follows
 * them without a continuity error. When that packet carries a payload, the
 * last init packet gets one less than its counter; when it carries only an
 * adaptation field (whose counter does not advance), the same counter.
 * Earlier init packets count back from there, a payload-bearing packet
 * stepping by one, and a duplicate packet (identical to the one before it,
 * 2.4.3.3) keeping the counter of its original. Packets on a PID absent from
 * first_payload are left alone. Both buffers are whole packets of
 * packet_size.
 */
MOQ_API moq_result_t moq_mpeg2ts_align_join_psi(uint8_t *psi_packets,
                                                size_t len,
                                                uint32_t packet_size,
                                                moq_bytes_t first_payload);

/* -- Subscriber: switching between alternate tracks --------------------- */

#define MOQ_MPEG2TS_SWITCHER_MAX_PMT 8u

/* Private per-PID section rewriting state. */
typedef struct moq_mpeg2ts_switch_pid {
    bool     used;
    uint16_t pid;
    /* Section in progress. */
    bool     in_section;
    bool     rewrite;
    bool     pat_copy;               /* collect the PAT to learn PMT PIDs */
    uint16_t sec_len;                /* 3 + section_length, 0 until known */
    uint16_t sec_pos;
    uint8_t  hdr[8];
    uint8_t  table_slot;
    uint32_t crc_orig;
    uint32_t crc_new;
    /* Last payload packet, to rewrite a duplicate identically. */
    bool     have_last;
    uint8_t  last_cc;
    bool     last_pusi;              /* its payload_unit_start_indicator */
    uint8_t  last_in[184];           /* its payload as received */
    uint8_t  last_out_len;
    uint8_t  last_out[184];          /* its payload as emitted */
} moq_mpeg2ts_switch_pid_t;

/* Private per-table version mapping (PAT, and PMT per program_number). */
typedef struct moq_mpeg2ts_switch_table {
    bool     used;
    uint8_t  table_id;
    uint16_t ext;
    bool     have_out;
    uint8_t  out_version;            /* last current version emitted */
    bool     offset_set;
    uint8_t  offset;
} moq_mpeg2ts_switch_table_t;

typedef struct moq_mpeg2ts_switcher_stats {
    uint32_t struct_size;
    uint64_t switches;
    uint64_t sections_rewritten;     /* version_number and CRC_32 changed */
    uint64_t pcr_discontinuities_set;
    uint64_t first_packet_di_set;    /* other PIDs (SHOULD) */
    uint64_t first_packet_di_skipped;/* no adaptation field to carry it */
    uint64_t pmt_pids_untracked;     /* PAT entries beyond the PMT limit */
} moq_mpeg2ts_switcher_stats_t;

/*
 * Rewrites the output packet stream across switches between alternate
 * tracks (Switching and Alternate Renditions). After each switch it
 *   - sets the discontinuity_indicator in the first packet that carries a
 *     PCR (MUST);
 *   - sets it in the first packet of every other PID when that packet has
 *     an adaptation field to carry it (SHOULD); a packet without one is
 *     passed unchanged and counted, because adding an adaptation field
 *     would move its payload;
 *   - re-versions every PAT section and every PMT section of the new track,
 *     whole tables spanning packets and sections packed in one packet
 *     included: the first current table after the switch gets the version
 *     after the one last emitted for that table, and later sections keep the
 *     same offset, with CRC_32 rewritten (MUST). A section whose CRC_32 was
 *     bad stays bad. Future tables (current_next_indicator 0) seen before
 *     the current one take a version that neither the last emitted table nor
 *     the next current one uses.
 * Tables are recognised by PID and table_id: table_id 0x00 on PID 0, 0x02 on
 * a PMT PID that the new track's PAT lists, or that the caller names at the
 * switch; other sections (private sections on a PMT PID, say) pass
 * unchanged. PMT PIDs learnt from the previous track are forgotten at the
 * switch, so a PID it used for its PMT can carry media in the new track. A
 * PMT that precedes the new PAT is recognised only when the caller names its
 * PID at the switch (from the catalog initialization data, say).
 * Private fields.
 */
typedef struct moq_mpeg2ts_switcher {
    uint32_t                     struct_size;
    uint32_t                     _packet_size;
    bool                         _switched;
    bool                         _pcr_pending;
    moq_mpeg2ts_switcher_stats_t _stats;
    moq_mpeg2ts_switch_pid_t     _pids[1 + MOQ_MPEG2TS_SWITCHER_MAX_PMT];
    moq_mpeg2ts_switch_table_t   _tables[1 + MOQ_MPEG2TS_SWITCHER_MAX_PMT];
    uint8_t                      _seen[(MOQ_MPEG2TS_PID_MAX + 1u) / 8u];
    uint8_t                      _pat_sec[MOQ_MPEG2TS_SECTION_MAX];
} moq_mpeg2ts_switcher_t;

MOQ_API moq_result_t moq_mpeg2ts_switcher_init(moq_mpeg2ts_switcher_t *sw,
                                               size_t sw_size,
                                               uint32_t packet_size);

/* The output moves to a different track from the next packet on. pmt_pids
 * (may be NULL with count 0) names the new track's PMT PIDs when the caller
 * knows them; at most MOQ_MPEG2TS_SWITCHER_MAX_PMT are used. */
MOQ_API void moq_mpeg2ts_switcher_switch(moq_mpeg2ts_switcher_t *sw,
                                         const uint16_t *pmt_pids,
                                         size_t pmt_pid_count);

/*
 * Rewrite one output packet in place. Returns MOQ_OK, MOQ_ERR_INVAL for NULL
 * arguments, and MOQ_ERR_PROTO for a packet that cannot be parsed or whose
 * pointer_field overruns it; such a packet is left unchanged and the
 * switcher's state is untouched, so a pending discontinuity_indicator still
 * goes on the next accepted packet carrying a PCR.
 */
MOQ_API moq_result_t moq_mpeg2ts_switcher_process(moq_mpeg2ts_switcher_t *sw,
                                                  uint8_t *packet);

/* Copy the counters: at most out_size octets, out->struct_size set to the
 * number written. Returns MOQ_ERR_INVAL for NULL arguments, an
 * uninitialised switcher, or an out_size below the first published size. */
MOQ_API moq_result_t moq_mpeg2ts_switcher_get_stats(
    const moq_mpeg2ts_switcher_t *sw,
    moq_mpeg2ts_switcher_stats_t *out, size_t out_size);

/* -- Egress timing ------------------------------------------------------ */

/*
 * Arithmetic for an egress scheduler; scheduling delivery is the
 * subscriber's (Egress Timing: a subscriber whose receiver recovers its clock
 * from packet arrival MUST deliver on a schedule consistent with the PCR).
 *
 * PCR value of the octet at byte_i, interpolated between the PCRs at byte_a
 * and byte_b (ISO/IEC 13818-1, 2.4.2.2), or extrapolated past byte_b at the
 * same rate. Byte positions count 188-octet TS packet octets (not M2TS
 * prefixes). Handles a PCR wrap between a and b. Returns MOQ_ERR_INVAL when
 * byte_b <= byte_a or byte_i < byte_a.
 */
MOQ_API moq_result_t moq_mpeg2ts_pcr_interpolate(uint64_t pcr_a,
                                                 uint64_t byte_a,
                                                 uint64_t pcr_b,
                                                 uint64_t byte_b,
                                                 uint64_t byte_i,
                                                 uint64_t *pcr_i);

/*
 * Null packets to insert so that `packets_sent` 188-octet packets over
 * `elapsed_27mhz` reach `mux_rate_bps` (mpeg2tsMuxRate as a stuffing target,
 * Egress Timing; the caller applies it only outside the unmodified modes).
 * 0 when the stream is already at or above the rate.
 */
MOQ_API uint64_t moq_mpeg2ts_stuffing_packets(uint64_t mux_rate_bps,
                                              uint64_t elapsed_27mhz,
                                              uint64_t packets_sent);

/* Write one null packet (PID 0x1FFF, payload 0xFF; zero prefix for 192, which
 * an "arrival-time" track must replace with a scheduled arrival time). */
MOQ_API moq_result_t moq_mpeg2ts_null_packet(uint32_t packet_size,
                                             uint8_t *out, size_t cap);

/* -- Publisher: per-program filter (Per-Program) ------------------------ */

#define MOQ_MPEG2TS_FILTER_MAX_SI_PIDS 16u

typedef struct moq_mpeg2ts_program_filter_cfg {
    uint32_t        struct_size;
    uint32_t        packet_size;
    uint16_t        program_number;  /* the program to carry, 1..65535 */
    bool            keep_null;       /* null packets MAY be kept */
    bool            keep_cat;        /* CAT and its EMM PIDs; default true */
    /* SI PIDs the publisher retains, copied at init (at most
     * MOQ_MPEG2TS_FILTER_MAX_SI_PIDS). Also the way to keep any other PID a
     * conditional access system needs that neither the PMT nor the CAT
     * names. */
    const uint16_t *si_pids;
    size_t          si_pid_count;
} moq_mpeg2ts_program_filter_cfg_t;

#define MOQ_MPEG2TS_PROGRAM_FILTER_CFG_V0_SIZE \
    (offsetof(moq_mpeg2ts_program_filter_cfg_t, si_pid_count) + \
     sizeof(size_t))

/* 188-octet packets, keep_cat true, no SI PIDs. */
MOQ_API void moq_mpeg2ts_program_filter_cfg_init_sized(
    moq_mpeg2ts_program_filter_cfg_t *cfg, size_t cfg_size);
#define moq_mpeg2ts_program_filter_cfg_init(cfg) \
    moq_mpeg2ts_program_filter_cfg_init_sized((cfg), sizeof(*(cfg)))

/* Caller-owned filter state. Private fields. Its size is dominated by the
 * per-PID bitmaps and the PSI section stores. */
typedef struct moq_mpeg2ts_program_filter {
    uint32_t                    struct_size;
    uint32_t                    _packet_size;
    uint16_t                    _program_number;
    bool                        _keep_null;
    bool                        _keep_cat;
    uint16_t                    _si_pids[MOQ_MPEG2TS_FILTER_MAX_SI_PIDS];
    size_t                      _si_pid_count;
    /* The program in the current PAT. */
    bool                        _program_present;
    bool                        _program_gone;
    uint16_t                    _pmt_pid;
    /* Version of the PAT being collected section by section. */
    bool                        _pat_building;
    uint8_t                     _pat_build_version;
    uint8_t                     _pat_build_last;
    bool                        _pat_build_found;
    uint8_t                     _pat_build_mask[32];
    /* PMT of the program. */
    bool                        _have_pmt;
    uint8_t                     _pmt_version;
    uint16_t                    _pcr_pid;
    /* Rewritten PAT. */
    bool                        _pat_emitted;
    uint8_t                     _pat_version;
    uint8_t                     _pat_cc;
    uint16_t                    _pat_tsid;
    uint16_t                    _pat_pmt_pid;
    /* CAT: EMM PIDs of the current version and of the one being collected. */
    bool                        _cat_building;
    uint8_t                     _cat_build_version;
    uint8_t                     _cat_build_last;
    uint8_t                     _cat_build_mask[32];
    uint8_t                     _keep[(MOQ_MPEG2TS_PID_MAX + 1u) / 8u];
    uint8_t                     _emm[(MOQ_MPEG2TS_PID_MAX + 1u) / 8u];
    uint8_t                     _emm_next[(MOQ_MPEG2TS_PID_MAX + 1u) / 8u];
    moq_mpeg2ts_section_state_t _pat_s;
    moq_mpeg2ts_section_state_t _pmt_s;
    moq_mpeg2ts_section_state_t _cat_s;
    uint8_t                     _pat_storage[MOQ_MPEG2TS_SECTION_MAX];
    uint8_t                     _pmt_storage[MOQ_MPEG2TS_SECTION_MAX];
    uint8_t                     _cat_storage[MOQ_MPEG2TS_SECTION_MAX];
} moq_mpeg2ts_program_filter_t;

MOQ_API moq_result_t moq_mpeg2ts_program_filter_init(
    moq_mpeg2ts_program_filter_t *f, size_t f_size,
    const moq_mpeg2ts_program_filter_cfg_t *cfg);

typedef enum moq_mpeg2ts_filter_action {
    MOQ_MPEG2TS_FILTER_DROP    = 0, /* nothing to emit */
    MOQ_MPEG2TS_FILTER_PASS    = 1, /* out holds the input packet unchanged */
    MOQ_MPEG2TS_FILTER_PAT     = 2, /* out holds the rewritten PAT packet */
} moq_mpeg2ts_filter_action_t;

/*
 * Filter one source packet (Per-Program: the packets a per-program track
 * SHOULD keep). Keeps
 *   - the PAT, rewritten to list only the carried program, CRC_32
 *     recomputed;
 *   - the PMT of the program and every PID its current PMT lists: PCR_PID,
 *     elementary streams, and the CA_PID of every CA_descriptor in either
 *     descriptor loop;
 *   - the CAT and every EMM PID its CA_descriptors name (when keep_cat), so
 *     the conditional access packets a descrambler needs stay in the track
 *     (Per-Program: MUST retain them); the CAT itself is passed unchanged,
 *     so it still lists the CA systems of every program;
 *   - the configured SI PIDs, unchanged, and null packets when asked.
 *
 * PAT and CAT are tracked as whole tables: every section of a version is
 * collected, sections packed in one packet included, and future tables
 * (current_next_indicator 0) are ignored. The program counts as gone only
 * when every section of a current PAT version has been seen without it.
 * Until a new version is complete the previous one stays in force, except
 * that a current section listing the program takes effect at once.
 *
 * The rewritten PAT has its own version_number and continuity counter: the
 * version starts at 0 and is incremented exactly when the rewritten content
 * (transport_stream_id, program, PMT PID) changes (Per-Program: MUST
 * increment when the content changes, MUST NOT otherwise). One rewritten PAT
 * is emitted for each source packet that carries the current section listing
 * the program, so the source's repetition rate is kept.
 *
 * out must hold one packet. Returns MOQ_OK, MOQ_ERR_INVAL for NULL arguments
 * or a short out, MOQ_ERR_PROTO for an unparseable packet (dropped).
 */
MOQ_API moq_result_t moq_mpeg2ts_program_filter_push(
    moq_mpeg2ts_program_filter_t *f,
    moq_bytes_t in_packet,
    uint8_t *out, size_t out_cap,
    moq_mpeg2ts_filter_action_t *action);

/* True once every section of a current PAT version was seen and none lists
 * the program (Per-Program: the publisher SHOULD then end the track). */
MOQ_API bool moq_mpeg2ts_program_filter_program_gone(
    const moq_mpeg2ts_program_filter_t *f);

/* -- Publisher: packetizer (Group Boundaries) --------------------------- */

typedef struct moq_mpeg2ts_emitted_object {   /* frozen layout */
    moq_bytes_t payload;             /* valid only during the callback */
    uint64_t    group;               /* 0-based Group index */
    uint64_t    object;              /* 0-based Object index in the Group */
    bool        new_group;           /* first Object of its Group */
    bool        random_access;       /* contains a random access point */
    size_t      packet_count;
} moq_mpeg2ts_emitted_object_t;

typedef void (*moq_mpeg2ts_emit_fn)(void *ctx,
                                    const moq_mpeg2ts_emitted_object_t *obj);

typedef struct moq_mpeg2ts_packetizer_cfg {
    uint32_t            struct_size;
    uint32_t            packet_size;
    /* Source packets per Object at most (>= 1). */
    uint32_t            object_packets;
    /* Start a Group at random access points of random_access_pid. */
    bool                group_at_random_access;
    /* Default PID_AUTO, the program's video PID; PID_NONE: any PID. */
    uint16_t            random_access_pid;
    /* Program whose PMT names the video PID for PID_AUTO. 0: the first
     * program of the PAT. */
    uint16_t            program_number;
    /* Do not start a Group sooner than this after the previous one (PCR
     * time); 0 starts one at every random access point. */
    uint32_t            min_group_ms;
    /* Discard source packets before the first random access point, so that
     * every Group starts with one. An unmodified-mode publisher that must
     * forward every source packet sets this false. */
    bool                wait_for_random_access;
    /* PCR PID for durations and rate. PID_NONE: the first PCR PID seen. */
    uint16_t            pcr_pid;
    /* Caller storage. object_buf holds object_packets packets. lead_buf
     * holds the PAT/PMT run that precedes a random access point, so that it
     * opens the new Group together with that point in its first Object; at
     * most object_packets - 1 packets of it are used (cap 0 disables it). A
     * run longer than that stays in the previous Group. */
    uint8_t            *object_buf;
    size_t              object_cap;
    uint8_t            *lead_buf;
    size_t              lead_cap;
    moq_mpeg2ts_emit_fn emit;
    void               *ctx;
} moq_mpeg2ts_packetizer_cfg_t;

/* 188-octet packets, 64 per Object, Groups at random access points of the
 * video PID, waiting for the first one, no lead-in storage. */
#define MOQ_MPEG2TS_PACKETIZER_CFG_V0_SIZE \
    (offsetof(moq_mpeg2ts_packetizer_cfg_t, ctx) + sizeof(void *))

MOQ_API void moq_mpeg2ts_packetizer_cfg_init_sized(
    moq_mpeg2ts_packetizer_cfg_t *cfg, size_t cfg_size);
#define moq_mpeg2ts_packetizer_cfg_init(cfg) \
    moq_mpeg2ts_packetizer_cfg_init_sized((cfg), sizeof(*(cfg)))

typedef struct moq_mpeg2ts_packetizer_stats {
    uint32_t struct_size;
    uint64_t groups;
    uint64_t objects;
    uint64_t packets_in;
    uint64_t packets_dropped_before_random_access;
    /* Longest Group in PCR time, and Groups longer than 2 s (Group
     * Boundaries: a Group SHOULD NOT last longer than 2 seconds). */
    uint64_t max_group_27mhz;
    uint64_t groups_over_2s;
    /* Peak rate between consecutive PCRs, counting 188-octet packets: the
     * floor for a declared mpeg2tsMuxRate (Mux Rate). */
    uint64_t peak_rate_bps;
    /* Source discontinuity_indicator on a PCR packet inside a Group. Passed
     * through unchanged; never introduced. */
    uint64_t source_pcr_discontinuities;
} moq_mpeg2ts_packetizer_stats_t;

/* Caller-owned packetizer state. Private fields. Its size is dominated by
 * the per-PID random access bitmap and the two PSI section stores. */
typedef struct moq_mpeg2ts_packetizer {
    uint32_t                         struct_size;
    moq_mpeg2ts_packetizer_cfg_t     _cfg;
    moq_mpeg2ts_packetizer_stats_t   _stats;
    size_t                           _lead_cap;   /* usable lead-in octets */
    bool                             _started;
    bool                             _group_open;
    bool                             _obj_rap;
    uint64_t                         _group;
    uint64_t                         _object;
    size_t                           _obj_len;
    size_t                           _lead_len;
    bool                             _lead_active;
    bool                             _have_pcr_pid;
    uint16_t                         _pcr_pid;
    bool                             _have_last_pcr;
    uint64_t                         _last_pcr;
    uint64_t                         _bytes_since_pcr;
    bool                             _have_cur_pcr;
    uint64_t                         _cur_pcr;
    bool                             _have_group_pcr;
    uint64_t                         _group_start_pcr;
    uint64_t                         _group_last_pcr;
    uint8_t                          _armed[(MOQ_MPEG2TS_PID_MAX + 1u) / 8u];
    bool                             _sel_have;
    uint8_t                          _sel_version;
    uint8_t                          _sel_section;
    uint16_t                         _sel_program;
    uint16_t                         _pmt_pid;
    bool                             _have_pmt;
    uint16_t                         _video_pid;
    moq_mpeg2ts_section_state_t      _pat_s;
    moq_mpeg2ts_section_state_t      _pmt_s;
    uint8_t                          _pat_store[MOQ_MPEG2TS_SECTION_MAX];
    uint8_t                          _pmt_store[MOQ_MPEG2TS_SECTION_MAX];
} moq_mpeg2ts_packetizer_t;

MOQ_API moq_result_t moq_mpeg2ts_packetizer_init(
    moq_mpeg2ts_packetizer_t *pz, size_t pz_size,
    const moq_mpeg2ts_packetizer_cfg_t *cfg);

/*
 * Feed one source packet. Packets are never split or modified. Completed
 * Objects are handed to cfg->emit in order. A Group opened at a random
 * access point has that point in its first Object, after the lead-in run
 * (Random Access: the first Object of every Group contains it). Returns
 * MOQ_OK, MOQ_ERR_INVAL for NULL arguments or a wrong length, MOQ_ERR_PROTO
 * for a packet without the sync byte (not taken).
 */
MOQ_API moq_result_t moq_mpeg2ts_packetizer_push(moq_mpeg2ts_packetizer_t *pz,
                                                 moq_bytes_t packet);

/*
 * End the Object being filled: emit it now, inside the open Group, even when
 * it holds fewer than object_packets packets. A PAT/PMT run held in the
 * lead-in stays held, to open the next Group with its random access point,
 * and the Group stays open: its statistics are not closed. Nothing is
 * emitted when the Object is empty. Returns MOQ_OK, or MOQ_ERR_INVAL for a
 * NULL or uninitialised packetizer.
 */
MOQ_API moq_result_t moq_mpeg2ts_packetizer_end_object(
    moq_mpeg2ts_packetizer_t *pz);

/* Emit everything still held (end of the source): the lead-in run goes into
 * the open Group, which is closed. */
MOQ_API moq_result_t moq_mpeg2ts_packetizer_flush(moq_mpeg2ts_packetizer_t *pz);

/* Copy the counters: at most out_size octets, out->struct_size set to the
 * number written. Returns MOQ_ERR_INVAL for NULL arguments, an
 * uninitialised packetizer, or an out_size below the first published size. */
MOQ_API moq_result_t moq_mpeg2ts_packetizer_get_stats(
    const moq_mpeg2ts_packetizer_t *pz,
    moq_mpeg2ts_packetizer_stats_t *out, size_t out_size);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_MPEG2TS_H */
