/*
 * Stream-level tests for moq-mpeg2ts: PSI reassembly across packets, the
 * per-program filter on multi-section and packed tables, conditional access
 * retention, the switcher on whole tables, the receiver's gap, group-end and
 * reorder handling, the packetizer's Group openings, and the bounds of every
 * caller-sized table and input struct.
 */

#include <moq/mpeg2ts.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ts_builders.h"

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

static moq_bytes_t B(const uint8_t *d, size_t n)
{
    moq_bytes_t b = { d, n };
    return b;
}

/* One 188-octet packet on `pid` carrying `sec` after pointer_field 0. */
static void psi_pkt(uint8_t *out, uint16_t pid, uint8_t cc, const uint8_t *sec,
                    size_t n)
{
    uint8_t pl[188];
    size_t pn = tb_psi_payload(pl, sec, n);
    tb_pkt_t p = { .pid = pid, .pusi = true, .cc = cc, .payload = pl,
                   .payload_len = pn };
    tb_packet(out, &p);
}

/*
 * Lay sections back to back on one PID as ISO/IEC 13818-1 2.4.4 carries
 * them: a packet in which a section begins has payload_unit_start and a
 * pointer_field to the first such section; the tail of the last packet is
 * 0xFF stuffing. Returns the number of packets written.
 */
static size_t pack_sections(uint8_t *out, size_t cap_packets, uint16_t pid,
                            uint8_t cc0, const uint8_t *const *secs,
                            const size_t *lens, size_t n)
{
    uint8_t stream[4096];
    size_t starts[16], total = 0;
    for (size_t i = 0; i < n; i++) {
        starts[i] = total;
        memcpy(stream + total, secs[i], lens[i]);
        total += lens[i];
    }
    size_t pos = 0, k = 0;
    uint8_t cc = cc0;
    while (pos < total && k < cap_packets) {
        /* Does a section begin within the next 183 octets? */
        size_t first = SIZE_MAX;
        for (size_t i = 0; i < n; i++)
            if (starts[i] >= pos && starts[i] < pos + 183 && first == SIZE_MAX)
                first = starts[i];
        uint8_t pl[184];
        memset(pl, 0xFF, sizeof(pl));
        size_t room;
        uint8_t *d;
        bool pusi = first != SIZE_MAX;
        if (pusi) {
            pl[0] = (uint8_t)(first - pos);
            d = pl + 1;
            room = 183;
        } else {
            d = pl;
            room = 184;
        }
        size_t take = total - pos < room ? total - pos : room;
        memcpy(d, stream + pos, take);
        pos += take;
        tb_pkt_t p = { .pid = pid, .pusi = pusi, .cc = cc, .payload = pl,
                       .payload_len = 184 };
        tb_packet(out + k * 188, &p);
        cc = (uint8_t)((cc + 1) & 0x0F);
        k++;
    }
    return k;
}

/* Sections returned by one push, copied out of their borrowed spans. */
typedef struct got {
    size_t  n;
    size_t  len[8];
    uint8_t sec[8][MOQ_MPEG2TS_SECTION_MAX];
} got_t;

static void feed(moq_mpeg2ts_section_buf_t *sb, const uint8_t *pkt, got_t *g)
{
    moq_mpeg2ts_packet_t p;
    moq_mpeg2ts_packet_init(&p);
    CHECK(moq_mpeg2ts_parse_packet(B(pkt, 188), 188, &p) == MOQ_OK);
    CHECK(moq_mpeg2ts_section_buf_push(sb, &p) == MOQ_OK);
    moq_bytes_t s;
    while (moq_mpeg2ts_section_buf_next(sb, &s) == MOQ_OK) {
        if (g->n < 8) {
            g->len[g->n] = s.len;
            memcpy(g->sec[g->n], s.data, s.len);
        }
        g->n++;
    }
}

static size_t pmt_with_descs(uint8_t *sec, uint8_t version, size_t desc_len)
{
    static uint8_t big[1000];
    for (size_t i = 0; i + 1 < sizeof(big); i += 2) {
        big[i] = 0x80;                    /* user-private descriptor */
        big[i + 1] = 0;
    }
    tb_es_t es[] = { { 0x1B, 0x0101, big, desc_len } };
    return tb_pmt(sec, 1, version, 0x0101, NULL, 0, es, 1);
}

/* A short private section (table_id 0x80, section_syntax_indicator 0) of
 * total length n >= 4. */
static size_t private_section(uint8_t *sec, size_t n)
{
    memset(sec, 0x5A, n);
    sec[0] = 0x80;
    sec[1] = (uint8_t)(0x70 | (((n - 3) >> 8) & 0x0F));
    sec[2] = (uint8_t)(n - 3);
    return n;
}

/* -- Section reassembly ------------------------------------------------- */

static void test_section_buf_packed(void)
{
    uint8_t storage[MOQ_MPEG2TS_SECTION_MAX];
    moq_mpeg2ts_section_buf_t sb;
    static got_t g;

    /* Two whole sections packed after one pointer_field. */
    {
        uint8_t a[64], b[64], pk[188];
        tb_prog_t tp[] = { { 1, 0x100 } };
        size_t an = tb_pat(a, 1, 0, tp, 1);
        tb_es_t es[] = { { 0x1B, 0x101, NULL, 0 } };
        size_t bn = tb_pmt(b, 1, 0, 0x101, NULL, 0, es, 1);
        const uint8_t *secs[] = { a, b };
        size_t lens[] = { an, bn };
        CHECK(pack_sections(pk, 1, 0x100, 0, secs, lens, 2) == 1);
        moq_mpeg2ts_section_buf_init(&sb, sizeof(sb), storage, sizeof(storage));
        memset(&g, 0, sizeof(g));
        feed(&sb, pk, &g);
        CHECK(g.n == 2);
        CHECK(g.len[0] == an && memcmp(g.sec[0], a, an) == 0);
        CHECK(g.len[1] == bn && memcmp(g.sec[1], b, bn) == 0);
    }

    /* A packet that completes one section through its pointer, carries a
     * whole second one and starts a third. */
    {
        uint8_t x[400], y[64], z[120], pk[188 * 4];
        size_t xn = pmt_with_descs(x, 0, 300);
        size_t yn = private_section(y, 21);
        size_t zn = private_section(z, 100);
        const uint8_t *secs[] = { x, y, z };
        size_t lens[] = { xn, yn, zn };
        size_t np = pack_sections(pk, 4, 0x100, 5, secs, lens, 3);
        CHECK(np == 3);
        moq_mpeg2ts_section_buf_init(&sb, sizeof(sb), storage, sizeof(storage));
        memset(&g, 0, sizeof(g));
        feed(&sb, pk, &g);
        CHECK(g.n == 0);
        feed(&sb, pk + 188, &g);
        CHECK(g.n == 2);
        CHECK(g.len[0] == xn && memcmp(g.sec[0], x, xn) == 0);
        CHECK(g.len[1] == yn && memcmp(g.sec[1], y, yn) == 0);
        feed(&sb, pk + 376, &g);
        CHECK(g.n == 3);
        CHECK(g.len[2] == zn && memcmp(g.sec[2], z, zn) == 0);
    }

    /* A legal duplicate of a continuation packet (same counter and payload,
     * 2.4.3.3) is ignored; the section completes once, byte-exact. */
    {
        uint8_t x[600], pk[188 * 3];
        size_t xn = pmt_with_descs(x, 0, 400);
        size_t pl = 0;
        uint8_t cc = 14;
        CHECK(moq_mpeg2ts_packetize_section(0x100, &cc, B(x, xn), 188, pk,
                                            sizeof(pk), &pl) == MOQ_OK);
        CHECK(pl == 188 * 3);
        static const int order[] = { 0, 1, 1, 2 };
        moq_mpeg2ts_section_buf_init(&sb, sizeof(sb), storage, sizeof(storage));
        memset(&g, 0, sizeof(g));
        for (size_t i = 0; i < 4; i++) feed(&sb, pk + order[i] * 188, &g);
        CHECK(g.n == 1);
        CHECK(g.len[0] == xn && memcmp(g.sec[0], x, xn) == 0);

        /* The same counter with a different payload is a discontinuity: the
         * partial section is abandoned, nothing completes. */
        uint8_t changed[188];
        memcpy(changed, pk + 188, 188);
        changed[100] ^= 0x01;
        moq_mpeg2ts_section_buf_reset(&sb);
        memset(&g, 0, sizeof(g));
        feed(&sb, pk, &g);
        feed(&sb, pk + 188, &g);
        feed(&sb, changed, &g);
        feed(&sb, pk + 376, &g);
        CHECK(g.n == 0);
    }

    /* A section whose header straddles two packets. */
    for (size_t fill = 176; fill <= 182; fill++) {
        uint8_t f[200], x[64], pk[188 * 2];
        size_t fn = private_section(f, fill);
        tb_es_t es[] = { { 0x1B, 0x101, NULL, 0 } };
        size_t xn = tb_pmt(x, 1, 3, 0x101, NULL, 0, es, 1);
        const uint8_t *secs[] = { f, x };
        size_t lens[] = { fn, xn };
        size_t np = pack_sections(pk, 2, 0x100, 0, secs, lens, 2);
        moq_mpeg2ts_section_buf_init(&sb, sizeof(sb), storage, sizeof(storage));
        memset(&g, 0, sizeof(g));
        for (size_t i = 0; i < np; i++) feed(&sb, pk + i * 188, &g);
        CHECK(g.n == 2);
        CHECK(g.len[1] == xn && memcmp(g.sec[1], x, xn) == 0);
    }
}

/* -- PSI parse validation -------------------------------------------------- */

static void test_pmt_validation(void)
{
    uint8_t sec[64];
    tb_es_t es[] = { { 0x1B, 0x101, NULL, 0 } };
    moq_mpeg2ts_stream_t st[2];
    moq_mpeg2ts_pmt_t pmt;
    moq_mpeg2ts_pmt_init(&pmt, st, 2);

    /* 2.4.4.9: a PMT is one section, number 0 of 0. */
    size_t n = tb_pmt(sec, 1, 0, 0x101, NULL, 0, es, 1);
    CHECK(moq_mpeg2ts_parse_pmt(B(sec, n), &pmt) == MOQ_OK);
    n = tb_section_numbers(sec, n, 1, 1);
    CHECK(moq_mpeg2ts_parse_pmt(B(sec, n), &pmt) == MOQ_ERR_PROTO);

    /* A descriptor that overruns its loop, with a correct CRC. */
    const uint8_t bad_desc[] = { 0x09, 0x04 };
    n = tb_pmt(sec, 1, 0, 0x101, bad_desc, sizeof(bad_desc), es, 1);
    CHECK(moq_mpeg2ts_parse_pmt(B(sec, n), &pmt) == MOQ_ERR_PROTO);
    tb_es_t es_bad[] = { { 0x1B, 0x101, bad_desc, sizeof(bad_desc) } };
    n = tb_pmt(sec, 1, 0, 0x101, NULL, 0, es_bad, 1);
    CHECK(moq_mpeg2ts_parse_pmt(B(sec, n), &pmt) == MOQ_ERR_PROTO);

    /* PAT section_number above last_section_number. */
    tb_prog_t tp[] = { { 1, 0x100 } };
    n = tb_pat(sec, 1, 0, tp, 1);
    n = tb_section_numbers(sec, n, 2, 1);
    moq_mpeg2ts_pat_t pat;
    moq_mpeg2ts_pat_init(&pat, NULL, 0);
    CHECK(moq_mpeg2ts_parse_pat(B(sec, n), &pat) == MOQ_ERR_PROTO);

    CHECK(moq_mpeg2ts_stream_type_is_video(0x1B));
    CHECK(moq_mpeg2ts_stream_type_is_video(0x24));
    CHECK(!moq_mpeg2ts_stream_type_is_video(0x0F));
    CHECK(!moq_mpeg2ts_stream_type_is_video(0x86));
}

/* -- Timing field validation ------------------------------------------- */

static void test_timing_validation(void)
{
    uint8_t pk[188];
    moq_mpeg2ts_packet_t p;

    /* program_clock_reference_extension counts 0..299 (2.4.3.5). */
    static const uint16_t exts[] = { 0, 299, 300, 511 };
    for (size_t i = 0; i < 4; i++) {
        tb_pkt_t s = { .pid = 0x100, .pcr = true, .pcr_base = 100,
                       .pcr_ext = exts[i] };
        tb_packet(pk, &s);
        moq_result_t rc = moq_mpeg2ts_parse_packet(B(pk, 188), 188, &p);
        CHECK(rc == (exts[i] < 300 ? MOQ_OK : MOQ_ERR_PROTO));
        if (rc == MOQ_OK) CHECK(p.has_pcr && p.pcr_ext == exts[i]);
    }
    CHECK(moq_mpeg2ts_pcr_27mhz(MOQ_MPEG2TS_PTS_WRAP - 1, 511) <
          MOQ_MPEG2TS_PCR_WRAP);

    /* Optional adaptation fields must fit the announced length (Table
     * 2-6): OPCR, splice_countdown, private data, extension. */
    struct { uint8_t flags; uint8_t af_len; uint8_t at5plus[8]; moq_result_t want; }
    af[] = {
        { 0x08, 1,  { 0 }, MOQ_ERR_PROTO },             /* OPCR, no room */
        { 0x08, 7,  { 0 }, MOQ_OK },
        { 0x04, 1,  { 0 }, MOQ_ERR_PROTO },             /* splice_countdown */
        { 0x04, 2,  { 0 }, MOQ_OK },
        { 0x02, 1,  { 0 }, MOQ_ERR_PROTO },             /* no length octet */
        { 0x02, 3,  { 0, 5 }, MOQ_ERR_PROTO },          /* 5 octets, room 1 */
        { 0x02, 3,  { 0, 1 }, MOQ_OK },
        { 0x01, 2,  { 0, 9 }, MOQ_ERR_PROTO },          /* extension overrun */
        { 0x01, 4,  { 0, 2 }, MOQ_OK },
        { 0x1F, 25, { 0 }, MOQ_OK },                    /* every field */
    };
    for (size_t i = 0; i < sizeof(af) / sizeof(af[0]); i++) {
        tb_pkt_t s = { .pid = 0x101, .af = true };
        tb_packet(pk, &s);
        pk[4] = af[i].af_len;
        memset(pk + 5, 0xFF, af[i].af_len);
        pk[5] = af[i].flags;
        if (af[i].flags == 0x1F) {
            memset(pk + 6, 0, 13);             /* PCR, OPCR, splice */
            pk[19] = 2;                        /* private data length */
            pk[22] = 2;                        /* extension length */
        } else {
            for (size_t k = 1; k < 8 && 5 + k < 188; k++)
                if (af[i].at5plus[k]) pk[5 + k] = af[i].at5plus[k];
        }
        CHECK(moq_mpeg2ts_parse_packet(B(pk, 188), 188, &p) == af[i].want);
    }

    /* PES_packet_length bounds the optional header (2.4.3.7). */
    const uint8_t short_pes[] = { 0x00, 0x00, 0x01, 0xE0, 0x00, 0x03, 0x80,
                                  0x80, 0x05, 0x21, 0x00, 0x01, 0x00, 0x01 };
    moq_mpeg2ts_pes_header_t h;
    CHECK(moq_mpeg2ts_parse_pes_header(B(short_pes, sizeof(short_pes)), &h) ==
          MOQ_ERR_PROTO);
    uint8_t ok_pes[sizeof(short_pes)];
    memcpy(ok_pes, short_pes, sizeof(ok_pes));
    ok_pes[5] = 8;                             /* 3 + 5 header octets */
    CHECK(moq_mpeg2ts_parse_pes_header(B(ok_pes, sizeof(ok_pes)), &h) == MOQ_OK);
    CHECK(h.has_pts && h.pts == 0);
    ok_pes[5] = 0;                             /* unbounded video form */
    CHECK(moq_mpeg2ts_parse_pes_header(B(ok_pes, sizeof(ok_pes)), &h) == MOQ_OK);
    ok_pes[4] = 0x10;                          /* long PES, partial payload */
    CHECK(moq_mpeg2ts_parse_pes_header(B(ok_pes, sizeof(ok_pes)), &h) == MOQ_OK);
}

/* -- Inspection: caller PMT table bounds -------------------------------- */

static void test_inspect_pmt_table_bounds(void)
{
    static const size_t counts[] = { 63, 64, 65 };
    static const size_t caps[] = { 0, 63, 64, 65, 66 };
    const uint8_t lang[] = { 0x0A, 0x04, 'e', 'n', 'g', 0x00 };
    for (size_t ci = 0; ci < 3; ci++) {
        size_t k = counts[ci];
        tb_es_t es[65];
        for (size_t i = 0; i < k; i++) {
            es[i].type = i == 0 ? 0x1B : 0x0F;
            es[i].pid = (uint16_t)(0x101 + i);
            es[i].desc = i == 0 ? NULL : lang;
            es[i].desc_len = i == 0 ? 0 : sizeof(lang);
        }
        uint8_t sec[1024];
        size_t sn = tb_pmt(sec, 1, 0, 0x101, NULL, 0, es, k);
        CHECK(sn <= 1024);
        uint8_t payload[188 * 8];
        size_t pl = 0;
        uint8_t cc = 0;
        CHECK(moq_mpeg2ts_packetize_section(0x100, &cc, B(sec, sn), 188,
                                            payload, sizeof(payload), &pl) ==
              MOQ_OK);
        for (size_t pi = 0; pi < 5; pi++) {
            size_t cap = caps[pi];
            moq_mpeg2ts_stream_t st[67];
            memset(st, 0xA5, sizeof(st));
            moq_mpeg2ts_pmt_t pmt;
            moq_mpeg2ts_pmt_init(&pmt, st, cap);
            moq_mpeg2ts_inspect_cfg_t ip;
            moq_mpeg2ts_inspect_cfg_init(&ip);
            ip.pmt_pid = 0x100;
            ip.program_number = 1;
            ip.pmt = &pmt;
            moq_mpeg2ts_object_info_t info;
            moq_result_t rc = moq_mpeg2ts_inspect_object(&ip, B(payload, pl), &info, sizeof(info));
            CHECK(rc == (k > cap ? MOQ_ERR_BUFFER : MOQ_OK));
            CHECK(info.has_pmt && info.pmt_video_pid == 0x101);
            CHECK(pmt.stream_count == k);
            CHECK(pmt.program_descriptors.data == NULL);
            size_t w = k < cap ? k : cap;
            for (size_t i = 0; i < w; i++) {
                CHECK(st[i].pid == 0x101 + i);
                CHECK(st[i].descriptors.data == NULL && st[i].descriptors.len == 0);
            }
            /* Nothing past the caller's table is written. */
            const uint8_t *canary = (const uint8_t *)&st[cap];
            for (size_t b = 0; b < sizeof(st[0]); b++) CHECK(canary[b] == 0xA5);
        }
    }
}

/* -- Sized inputs ---------------------------------------------------------- */

static void noop_emit(void *u, const moq_mpeg2ts_emitted_object_t *o)
{
    (void)u;
    (void)o;
}

static void test_sized_inputs(void)
{
    uint8_t pk[188];
    tb_pkt_t s = { .pid = 0x100 };
    tb_packet(pk, &s);

    moq_mpeg2ts_object_rules_t rules;
    moq_mpeg2ts_object_rules_init(&rules);
    moq_mpeg2ts_object_report_t rep;
    CHECK(moq_mpeg2ts_validate_object(&rules, B(pk, 188), &rep, sizeof(rep)) == MOQ_OK);
    rules.struct_size = (uint32_t)offsetof(moq_mpeg2ts_object_rules_t, es_pid);
    CHECK(moq_mpeg2ts_validate_object(&rules, B(pk, 188), &rep, sizeof(rep)) == MOQ_ERR_INVAL);

    moq_mpeg2ts_inspect_cfg_t ip;
    moq_mpeg2ts_inspect_cfg_init(&ip);
    moq_mpeg2ts_object_info_t info;
    CHECK(moq_mpeg2ts_inspect_object(&ip, B(pk, 188), &info, sizeof(info)) == MOQ_OK);
    ip.struct_size = (uint32_t)offsetof(moq_mpeg2ts_inspect_cfg_t, pmt);
    CHECK(moq_mpeg2ts_inspect_object(&ip, B(pk, 188), &info, sizeof(info)) == MOQ_ERR_INVAL);
    moq_mpeg2ts_inspect_cfg_init(&ip);
    ip.random_access_pid = 0x2000;
    CHECK(moq_mpeg2ts_inspect_object(&ip, B(pk, 188), &info, sizeof(info)) == MOQ_ERR_INVAL);

    moq_mpeg2ts_receiver_cfg_t rc;
    moq_mpeg2ts_receiver_cfg_init(&rc);
    CHECK(rc.random_access_pid == MOQ_MPEG2TS_PID_AUTO);
    static moq_mpeg2ts_receiver_t rx;
    rc.struct_size = (uint32_t)offsetof(moq_mpeg2ts_receiver_cfg_t,
                                        reorder_window_us);
    CHECK(moq_mpeg2ts_receiver_init(&rx, sizeof(rx), &rc) == MOQ_ERR_INVAL);
    moq_mpeg2ts_receiver_cfg_init(&rc);
    rc.reorder_slot_count = 2;                 /* slots missing */
    CHECK(moq_mpeg2ts_receiver_init(&rx, sizeof(rx), &rc) == MOQ_ERR_INVAL);

    moq_mpeg2ts_program_filter_cfg_t fc;
    moq_mpeg2ts_program_filter_cfg_init(&fc);
    fc.program_number = 1;
    static moq_mpeg2ts_program_filter_t f;
    CHECK(moq_mpeg2ts_program_filter_init(&f, sizeof(f), &fc) == MOQ_OK);
    fc.struct_size = (uint32_t)offsetof(moq_mpeg2ts_program_filter_cfg_t,
                                        si_pid_count);
    CHECK(moq_mpeg2ts_program_filter_init(&f, sizeof(f), &fc) == MOQ_ERR_INVAL);

    uint8_t ob[188];
    moq_mpeg2ts_packetizer_cfg_t pc;
    moq_mpeg2ts_packetizer_cfg_init(&pc);
    CHECK(pc.random_access_pid == MOQ_MPEG2TS_PID_AUTO);
    pc.object_packets = 1;
    pc.object_buf = ob;
    pc.object_cap = sizeof(ob);
    pc.emit = noop_emit;
    static moq_mpeg2ts_packetizer_t pz;
    CHECK(moq_mpeg2ts_packetizer_init(&pz, sizeof(pz), &pc) == MOQ_OK);
    pc.struct_size = (uint32_t)offsetof(moq_mpeg2ts_packetizer_cfg_t, ctx);
    CHECK(moq_mpeg2ts_packetizer_init(&pz, sizeof(pz), &pc) == MOQ_ERR_INVAL);

    static moq_mpeg2ts_switcher_t sw;
    CHECK(moq_mpeg2ts_switcher_init(&sw, sizeof(sw) - 1, 188) == MOQ_ERR_INVAL);
}

/* -- Program filter: whole tables ---------------------------------------- */

static moq_mpeg2ts_filter_action_t fpush(moq_mpeg2ts_program_filter_t *f,
                                         const uint8_t *in, uint8_t *out)
{
    uint8_t tmp[188];
    moq_mpeg2ts_filter_action_t a = MOQ_MPEG2TS_FILTER_DROP;
    CHECK(moq_mpeg2ts_program_filter_push(f, B(in, 188), out ? out : tmp, 188,
                                          &a) == MOQ_OK);
    return a;
}

static bool passes(moq_mpeg2ts_program_filter_t *f, uint16_t pid)
{
    uint8_t pk[188];
    tb_pkt_t s = { .pid = pid };
    tb_packet(pk, &s);
    return fpush(f, pk, NULL) == MOQ_MPEG2TS_FILTER_PASS;
}

static moq_mpeg2ts_program_filter_t *new_filter(uint16_t program, bool keep_cat)
{
    moq_mpeg2ts_program_filter_cfg_t cfg;
    moq_mpeg2ts_program_filter_cfg_init(&cfg);
    cfg.program_number = program;
    cfg.keep_cat = keep_cat;
    moq_mpeg2ts_program_filter_t *f = calloc(1, sizeof(*f));
    CHECK(moq_mpeg2ts_program_filter_init(f, sizeof(*f), &cfg) == MOQ_OK);
    return f;
}

static size_t pat_one(uint8_t *sec, uint8_t version, uint8_t number,
                      uint8_t last, uint16_t prog, uint16_t pmt_pid)
{
    tb_prog_t tp[] = { { prog, pmt_pid } };
    size_t n = tb_pat(sec, 0x42, version, tp, 1);
    return tb_section_numbers(sec, n, number, last);
}

static void feed_pmt(moq_mpeg2ts_program_filter_t *f, uint16_t pmt_pid,
                     uint8_t version, uint16_t video, uint8_t cc)
{
    uint8_t sec[64], pk[188];
    tb_es_t es[] = { { 0x1B, video, NULL, 0 } };
    size_t n = tb_pmt(sec, 1, version, video, NULL, 0, es, 1);
    psi_pkt(pk, pmt_pid, cc, sec, n);
    CHECK(fpush(f, pk, NULL) == MOQ_MPEG2TS_FILTER_PASS);
}

static void test_filter_multisection_pat(void)
{
    uint8_t s0[64], s1[64], pk[188], out[188];
    /* Section 0 lists program 2, section 1 lists program 1: both orders. */
    for (int order = 0; order < 2; order++) {
        moq_mpeg2ts_program_filter_t *f = new_filter(1, true);
        size_t n0 = pat_one(s0, 7, 0, 1, 2, 0x200);
        size_t n1 = pat_one(s1, 7, 1, 1, 1, 0x100);
        uint8_t cc = 0;
        if (order == 0) {
            psi_pkt(pk, 0, cc++, s0, n0);
            CHECK(fpush(f, pk, out) == MOQ_MPEG2TS_FILTER_DROP);
            /* Discovery is not finished: the program is not gone. */
            CHECK(!moq_mpeg2ts_program_filter_program_gone(f));
            psi_pkt(pk, 0, cc++, s1, n1);
            CHECK(fpush(f, pk, out) == MOQ_MPEG2TS_FILTER_PAT);
        } else {
            psi_pkt(pk, 0, cc++, s1, n1);
            CHECK(fpush(f, pk, out) == MOQ_MPEG2TS_FILTER_PAT);
            psi_pkt(pk, 0, cc++, s0, n0);
            CHECK(fpush(f, pk, out) == MOQ_MPEG2TS_FILTER_DROP);
        }
        feed_pmt(f, 0x100, 0, 0x101, 0);
        CHECK(passes(f, 0x101));
        /* Repeats of the section without the program change nothing. */
        for (int r = 0; r < 3; r++) {
            psi_pkt(pk, 0, cc++, s0, n0);
            fpush(f, pk, out);
            CHECK(passes(f, 0x101));
            CHECK(!moq_mpeg2ts_program_filter_program_gone(f));
        }
        /* A new version, incomplete: the old one stays in force. */
        size_t m0 = pat_one(s0, 8, 0, 1, 2, 0x200);
        psi_pkt(pk, 0, cc++, s0, m0);
        fpush(f, pk, out);
        CHECK(passes(f, 0x101));
        CHECK(!moq_mpeg2ts_program_filter_program_gone(f));
        /* Its last section completes it without program 1: gone. */
        size_t m1 = pat_one(s1, 8, 1, 1, 3, 0x300);
        psi_pkt(pk, 0, cc++, s1, m1);
        CHECK(fpush(f, pk, out) == MOQ_MPEG2TS_FILTER_DROP);
        CHECK(moq_mpeg2ts_program_filter_program_gone(f));
        CHECK(!passes(f, 0x101));
        free(f);
    }

    /* A future PAT (current_next 0) is ignored. */
    {
        moq_mpeg2ts_program_filter_t *f = new_filter(1, true);
        size_t n = pat_one(s0, 0, 0, 0, 1, 0x100);
        psi_pkt(pk, 0, 0, s0, n);
        CHECK(fpush(f, pk, out) == MOQ_MPEG2TS_FILTER_PAT);
        n = pat_one(s0, 1, 0, 0, 2, 0x200);
        n = tb_section_current(s0, n, false);
        psi_pkt(pk, 0, 1, s0, n);
        CHECK(fpush(f, pk, out) == MOQ_MPEG2TS_FILTER_DROP);
        CHECK(!moq_mpeg2ts_program_filter_program_gone(f));
        free(f);
    }
}

static void test_filter_packed_pmt(void)
{
    moq_mpeg2ts_program_filter_t *f = new_filter(1, true);
    uint8_t sec[64], pk[188];
    size_t n = pat_one(sec, 0, 0, 0, 1, 0x100);
    psi_pkt(pk, 0, 0, sec, n);
    CHECK(fpush(f, pk, NULL) == MOQ_MPEG2TS_FILTER_PAT);

    /* Future PMT (lists 0x102) then current PMT (lists 0x101), packed, with
     * a private section first. */
    uint8_t priv[32], fut[64], cur[64];
    size_t pn = private_section(priv, 12);
    tb_es_t ef[] = { { 0x1B, 0x102, NULL, 0 } };
    tb_es_t ec[] = { { 0x1B, 0x101, NULL, 0 } };
    size_t fn = tb_pmt(fut, 1, 1, 0x102, NULL, 0, ef, 1);
    fn = tb_section_current(fut, fn, false);
    size_t cn = tb_pmt(cur, 1, 0, 0x101, NULL, 0, ec, 1);
    const uint8_t *secs[] = { priv, fut, cur };
    size_t lens[] = { pn, fn, cn };
    CHECK(pack_sections(pk, 1, 0x100, 0, secs, lens, 3) == 1);
    CHECK(fpush(f, pk, NULL) == MOQ_MPEG2TS_FILTER_PASS);
    CHECK(passes(f, 0x101));
    CHECK(!passes(f, 0x102));
    free(f);
}

static void test_filter_ca(void)
{
    uint8_t sec[1100], pk[188 * 4];

    /* The CAT's EMM PIDs are kept with the CAT; a new CAT version replaces
     * them once complete. */
    {
        moq_mpeg2ts_program_filter_t *f = new_filter(1, true);
        size_t n = pat_one(sec, 0, 0, 0, 1, 0x100);
        psi_pkt(pk, 0, 0, sec, n);
        fpush(f, pk, NULL);
        uint8_t ca[6];
        tb_ca_desc(ca, 0x0B00, 0x500);
        tb_es_t es[] = { { 0x1B, 0x101, NULL, 0 } };
        n = tb_pmt(sec, 1, 0, 0x101, ca, 6, es, 1);
        psi_pkt(pk, 0x100, 0, sec, n);
        fpush(f, pk, NULL);
        CHECK(passes(f, 0x500));               /* ECM, from the PMT */
        CHECK(!passes(f, 0x501));
        uint8_t emm[12];
        tb_ca_desc(emm, 0x0B00, 0x501);
        tb_ca_desc(emm + 6, 0x0100, 0x601);    /* another system's EMM */
        n = tb_cat(sec, 0, emm, 12);
        psi_pkt(pk, MOQ_MPEG2TS_PID_CAT, 0, sec, n);
        CHECK(fpush(f, pk, NULL) == MOQ_MPEG2TS_FILTER_PASS);
        CHECK(passes(f, 0x501));
        CHECK(passes(f, 0x601));
        tb_ca_desc(emm, 0x0B00, 0x502);
        n = tb_cat(sec, 1, emm, 6);
        psi_pkt(pk, MOQ_MPEG2TS_PID_CAT, 1, sec, n);
        fpush(f, pk, NULL);
        CHECK(passes(f, 0x502));
        CHECK(!passes(f, 0x501) && !passes(f, 0x601));
        free(f);

        /* keep_cat false: neither the CAT nor its EMM PIDs. */
        f = new_filter(1, false);
        n = tb_cat(sec, 0, emm, 6);
        psi_pkt(pk, MOQ_MPEG2TS_PID_CAT, 0, sec, n);
        CHECK(fpush(f, pk, NULL) == MOQ_MPEG2TS_FILTER_DROP);
        CHECK(!passes(f, 0x502));
        free(f);
    }

    /* 32 and 33 CA_descriptors, in either descriptor loop, and across a
     * PMT version change: every CA_PID is kept. */
    for (size_t count = 32; count <= 33; count++)
    for (int loop = 0; loop < 2; loop++) {
        moq_mpeg2ts_program_filter_t *f = new_filter(1, true);
        size_t n = pat_one(sec, 0, 0, 0, 1, 0x100);
        psi_pkt(pk, 0, 0, sec, n);
        fpush(f, pk, NULL);
        uint8_t desc[33 * 6];
        for (size_t i = 0; i < count; i++)
            tb_ca_desc(desc + i * 6, (uint16_t)(0x1000 + i),
                       (uint16_t)(0x500 + i));
        for (uint8_t version = 0; version < 2; version++) {
            tb_es_t es[] = { { 0x1B, 0x101, loop ? desc : NULL,
                               loop ? count * 6 : 0 } };
            n = tb_pmt(sec, 1, version, 0x101, loop ? NULL : desc,
                       loop ? 0 : count * 6, es, 1);
            size_t pl = 0;
            uint8_t cc = (uint8_t)(version * 4);
            CHECK(moq_mpeg2ts_packetize_section(0x100, &cc, B(sec, n), 188, pk,
                                                sizeof(pk), &pl) == MOQ_OK);
            for (size_t i = 0; i < pl / 188; i++) fpush(f, pk + i * 188, NULL);
            for (size_t i = 0; i < count; i++)
                CHECK(passes(f, (uint16_t)(0x500 + i)));
            CHECK(passes(f, 0x101));
        }
        free(f);
    }
}

/* The filter state holds no pointer into itself: a byte copy taken in the
 * middle of a section continues it. */
static void test_filter_relocation(void)
{
    moq_mpeg2ts_program_filter_t *f = new_filter(1, true);
    uint8_t pat[600], pk[188 * 4];
    tb_prog_t tp[100];
    for (size_t i = 0; i < 100; i++) {
        tp[i].number = (uint16_t)(i + 1);
        tp[i].pid = (uint16_t)(0x100 + i);
    }
    size_t n = tb_pat(pat, 1, 0, tp, 100);    /* 412 octets: three packets */
    size_t pl = 0;
    uint8_t cc = 0;
    CHECK(moq_mpeg2ts_packetize_section(0, &cc, B(pat, n), 188, pk, sizeof(pk),
                                        &pl) == MOQ_OK);
    CHECK(pl == 188 * 3);
    CHECK(fpush(f, pk, NULL) == MOQ_MPEG2TS_FILTER_DROP);
    moq_mpeg2ts_program_filter_t *g = malloc(sizeof(*g));
    memcpy(g, f, sizeof(*g));
    memset(f, 0xAA, sizeof(*f));
    free(f);
    CHECK(fpush(g, pk + 188, NULL) == MOQ_MPEG2TS_FILTER_DROP);
    uint8_t out[188];
    CHECK(fpush(g, pk + 376, out) == MOQ_MPEG2TS_FILTER_PAT);
    moq_mpeg2ts_program_t progs[2];
    moq_mpeg2ts_pat_t parsed;
    moq_mpeg2ts_pat_init(&parsed, progs, 2);
    CHECK(moq_mpeg2ts_parse_pat(B(out + 5, 16), &parsed) == MOQ_OK);
    CHECK(parsed.program_count == 1 && progs[0].pmt_pid == 0x100);
    free(g);
}

/* -- Switcher: whole tables ------------------------------------------------ */

static moq_mpeg2ts_switcher_t *new_switcher(void)
{
    moq_mpeg2ts_switcher_t *sw = calloc(1, sizeof(*sw));
    CHECK(moq_mpeg2ts_switcher_init(sw, sizeof(*sw), 188) == MOQ_OK);
    return sw;
}

static void sw_run(moq_mpeg2ts_switcher_t *sw, uint8_t *pk, size_t np)
{
    for (size_t i = 0; i < np; i++)
        CHECK(moq_mpeg2ts_switcher_process(sw, pk + i * 188) == MOQ_OK);
}

/* Track A's PSI, emitted before the switch: PAT v`pv` (program 1 on
 * `pmt_pid`), PMT v`mv`. */
static void sw_track_a(moq_mpeg2ts_switcher_t *sw, uint8_t pv, uint8_t mv,
                       uint16_t pmt_pid)
{
    uint8_t sec[64], pk[188];
    tb_prog_t tp[] = { { 1, pmt_pid } };
    size_t n = tb_pat(sec, 1, pv, tp, 1);
    psi_pkt(pk, 0, 0, sec, n);
    sw_run(sw, pk, 1);
    tb_es_t es[] = { { 0x1B, 0x101, NULL, 0 } };
    n = tb_pmt(sec, 1, mv, 0x101, NULL, 0, es, 1);
    psi_pkt(pk, pmt_pid, 0, sec, n);
    sw_run(sw, pk, 1);
}

static void test_switcher_tables(void)
{
    static got_t g;
    uint8_t storage[MOQ_MPEG2TS_SECTION_MAX];
    moq_mpeg2ts_section_buf_t sb;

    /* A PMT spanning three packets, placed after a private section of every
     * length from 1 to 200 octets so that its header and its CRC_32 fall on
     * every packet boundary. The whole re-versioned section is checked. */
    for (size_t fill = 4; fill <= 200; fill++) {
        moq_mpeg2ts_switcher_t *sw = new_switcher();
        sw_track_a(sw, 0, 0, 0x100);
        moq_mpeg2ts_switcher_switch(sw, NULL, 0);
        uint8_t sec[64], pk[188 * 6];
        tb_prog_t tp[] = { { 1, 0x100 } };
        size_t n = tb_pat(sec, 1, 0, tp, 1);
        psi_pkt(pk, 0, 1, sec, n);
        sw_run(sw, pk, 1);
        uint8_t priv[256], x[600];
        size_t pn = private_section(priv, fill);
        size_t xn = pmt_with_descs(x, 0, 380);
        const uint8_t *secs[] = { priv, x };
        size_t lens[] = { pn, xn };
        size_t np = pack_sections(pk, 6, 0x100, 1, secs, lens, 2);
        sw_run(sw, pk, np);
        moq_mpeg2ts_section_buf_init(&sb, sizeof(sb), storage, sizeof(storage));
        memset(&g, 0, sizeof(g));
        for (size_t i = 0; i < np; i++) feed(&sb, pk + i * 188, &g);
        CHECK(g.n == 2);
        CHECK(g.len[0] == pn && memcmp(g.sec[0], priv, pn) == 0);
        CHECK(g.len[1] == xn && tb_crc32(g.sec[1], xn) == 0);
        CHECK(((g.sec[1][5] >> 1) & 0x1F) == 1);
        /* Only the version and the CRC_32 changed. */
        CHECK(memcmp(g.sec[1], x, 5) == 0 && memcmp(g.sec[1] + 6, x + 6, xn - 10) == 0);
        free(sw);
    }

    /* Two PAT sections packed in one packet take the same new version. */
    {
        moq_mpeg2ts_switcher_t *sw = new_switcher();
        sw_track_a(sw, 0, 0, 0x100);
        moq_mpeg2ts_switcher_switch(sw, NULL, 0);
        uint8_t a[64], b[64], pk[188];
        tb_prog_t t0[] = { { 0, 0x10 } }, t1[] = { { 1, 0x100 } };
        size_t an = tb_pat(a, 1, 0, t0, 1);
        an = tb_section_numbers(a, an, 0, 1);
        size_t bn = tb_pat(b, 1, 0, t1, 1);
        bn = tb_section_numbers(b, bn, 1, 1);
        const uint8_t *secs[] = { a, b };
        size_t lens[] = { an, bn };
        pack_sections(pk, 1, 0, 1, secs, lens, 2);
        sw_run(sw, pk, 1);
        const uint8_t *s0 = pk + 5, *s1 = pk + 5 + an;
        CHECK(((s0[5] >> 1) & 0x1F) == 1 && ((s1[5] >> 1) & 0x1F) == 1);
        CHECK(tb_crc32(s0, an) == 0 && tb_crc32(s1, bn) == 0);
        free(sw);
    }

    /* Future then current after a switch: the current table differs from
     * the last emitted one, the future one from both. */
    {
        moq_mpeg2ts_switcher_t *sw = new_switcher();
        sw_track_a(sw, 0, 0, 0x100);
        moq_mpeg2ts_switcher_switch(sw, NULL, 0);
        uint8_t fut[64], cur[64], pk[188];
        tb_prog_t tb2[] = { { 1, 0x200 } };
        size_t fn = tb_pat(fut, 1, 1, tb2, 1);
        fn = tb_section_current(fut, fn, false);
        psi_pkt(pk, 0, 1, fut, fn);
        sw_run(sw, pk, 1);
        int vf = (pk[5 + 5] >> 1) & 0x1F;
        CHECK(tb_crc32(pk + 5, fn) == 0);
        size_t cn = tb_pat(cur, 1, 0, tb2, 1);
        psi_pkt(pk, 0, 2, cur, cn);
        sw_run(sw, pk, 1);
        int vc = (pk[5 + 5] >> 1) & 0x1F;
        CHECK(vc != 0 && vf != 0 && vf != vc);
        /* The source then moves to version 1: still one ahead. */
        size_t n2 = tb_pat(cur, 1, 1, tb2, 1);
        psi_pkt(pk, 0, 3, cur, n2);
        sw_run(sw, pk, 1);
        CHECK(((pk[5 + 5] >> 1) & 0x1F) == ((vc + 1) & 0x1F));
        free(sw);
    }

    /* Version 31 to 0, and a bad CRC_32 stays bad. */
    {
        moq_mpeg2ts_switcher_t *sw = new_switcher();
        sw_track_a(sw, 31, 31, 0x100);
        moq_mpeg2ts_switcher_switch(sw, NULL, 0);
        uint8_t sec[64], pk[188];
        tb_prog_t tp[] = { { 1, 0x100 } };
        size_t n = tb_pat(sec, 1, 31, tp, 1);
        psi_pkt(pk, 0, 1, sec, n);
        sw_run(sw, pk, 1);
        CHECK(((pk[5 + 5] >> 1) & 0x1F) == 0 && tb_crc32(pk + 5, n) == 0);
        tb_es_t es[] = { { 0x1B, 0x101, NULL, 0 } };
        n = tb_pmt(sec, 1, 31, 0x101, NULL, 0, es, 1);
        sec[n - 1] ^= 0x01;                    /* damaged in the source */
        psi_pkt(pk, 0x100, 1, sec, n);
        sw_run(sw, pk, 1);
        CHECK(((pk[5 + 5] >> 1) & 0x1F) == 0);
        CHECK(tb_crc32(pk + 5, n) != 0);
        free(sw);
    }

    /* The old PMT PID carries video in the new track: passed unchanged. The
     * new PAT's PMT PID is then rewritten. A PMT named at the switch is
     * rewritten even before the new PAT. */
    {
        moq_mpeg2ts_switcher_t *sw = new_switcher();
        sw_track_a(sw, 0, 0, 0x100);
        moq_mpeg2ts_switcher_switch(sw, NULL, 0);
        uint8_t pes[32], pk[188], copy[188];
        size_t pn = tb_pes(pes, 0xE0, true, 9000, false, 0);
        tb_pkt_t v = { .pid = 0x100, .pusi = true, .payload = pes,
                       .payload_len = pn };
        tb_packet(pk, &v);
        memcpy(copy, pk, 188);
        CHECK(moq_mpeg2ts_switcher_process(sw, pk) == MOQ_OK);
        CHECK(memcmp(pk, copy, 188) == 0);
        uint8_t sec[64];
        tb_prog_t tp[] = { { 1, 0x200 } };
        size_t n = tb_pat(sec, 1, 0, tp, 1);
        psi_pkt(pk, 0, 1, sec, n);
        sw_run(sw, pk, 1);
        tb_es_t es[] = { { 0x1B, 0x201, NULL, 0 } };
        n = tb_pmt(sec, 1, 0, 0x201, NULL, 0, es, 1);
        psi_pkt(pk, 0x200, 0, sec, n);
        sw_run(sw, pk, 1);
        CHECK(((pk[5 + 5] >> 1) & 0x1F) == 1 && tb_crc32(pk + 5, n) == 0);

        const uint16_t hint[] = { 0x300 };
        moq_mpeg2ts_switcher_switch(sw, hint, 1);
        n = tb_pmt(sec, 1, 1, 0x301, NULL, 0, es, 1);
        psi_pkt(pk, 0x300, 0, sec, n);
        sw_run(sw, pk, 1);
        CHECK(((pk[5 + 5] >> 1) & 0x1F) == 2 && tb_crc32(pk + 5, n) == 0);
        free(sw);
    }

    /* A private section on a PMT PID passes unchanged, alone or packed
     * before the PMT, which is still rewritten. */
    {
        moq_mpeg2ts_switcher_t *sw = new_switcher();
        sw_track_a(sw, 0, 0, 0x100);
        uint8_t priv[32], pk[188], copy[188];
        size_t pn = private_section(priv, 12);
        psi_pkt(pk, 0x100, 1, priv, pn);
        memcpy(copy, pk, 188);
        CHECK(moq_mpeg2ts_switcher_process(sw, pk) == MOQ_OK);
        CHECK(memcmp(copy, pk, 188) == 0);
        moq_mpeg2ts_switcher_switch(sw, NULL, 0);
        uint8_t sec[64];
        tb_prog_t tp[] = { { 1, 0x100 } };
        size_t n = tb_pat(sec, 1, 0, tp, 1);
        psi_pkt(pk, 0, 1, sec, n);
        sw_run(sw, pk, 1);
        tb_es_t es[] = { { 0x1B, 0x101, NULL, 0 } };
        uint8_t pmt[64];
        size_t mn = tb_pmt(pmt, 1, 0, 0x101, NULL, 0, es, 1);
        const uint8_t *secs[] = { priv, pmt };
        size_t lens[] = { pn, mn };
        pack_sections(pk, 1, 0x100, 2, secs, lens, 2);
        sw_run(sw, pk, 1);
        CHECK(memcmp(pk + 5, priv, pn) == 0);
        CHECK(((pk[5 + pn + 5] >> 1) & 0x1F) == 1);
        CHECK(tb_crc32(pk + 5 + pn, mn) == 0);
        free(sw);
    }

    /* A duplicate PSI packet is rewritten exactly like its original. */
    {
        moq_mpeg2ts_switcher_t *sw = new_switcher();
        sw_track_a(sw, 0, 0, 0x100);
        moq_mpeg2ts_switcher_switch(sw, NULL, 0);
        uint8_t sec[64], pk[188], dup[188];
        tb_prog_t tp[] = { { 1, 0x100 } };
        size_t n = tb_pat(sec, 1, 0, tp, 1);
        psi_pkt(pk, 0, 1, sec, n);
        memcpy(dup, pk, 188);
        sw_run(sw, pk, 1);
        sw_run(sw, dup, 1);
        CHECK(memcmp(pk, dup, 188) == 0);
        free(sw);
    }
}

static void test_switcher_atomic(void)
{
    /* A packet refused for its pointer_field is left unchanged, and the
     * pending discontinuity_indicator goes on the next PCR. */
    moq_mpeg2ts_switcher_t *sw = new_switcher();
    moq_mpeg2ts_switcher_switch(sw, NULL, 0);
    uint8_t pk[188], copy[188];
    tb_pkt_t bad = { .pid = 0, .pusi = true, .pcr = true, .pcr_base = 5 };
    tb_packet(pk, &bad);
    pk[12] = 200;                              /* pointer_field overrun */
    memcpy(copy, pk, 188);
    CHECK(moq_mpeg2ts_switcher_process(sw, pk) == MOQ_ERR_PROTO);
    CHECK(memcmp(copy, pk, 188) == 0);
    tb_pkt_t v = { .pid = 0x101, .pcr = true, .pcr_base = 9 };
    tb_packet(pk, &v);
    CHECK(moq_mpeg2ts_switcher_process(sw, pk) == MOQ_OK);
    moq_mpeg2ts_packet_t p;
    moq_mpeg2ts_packet_init(&p);
    CHECK(moq_mpeg2ts_parse_packet(B(pk, 188), 188, &p) == MOQ_OK);
    CHECK(p.discontinuity);

    /* A PAT with a damaged CRC on a PCR packet: accepted, discontinuity set,
     * the section still damaged. */
    moq_mpeg2ts_switcher_switch(sw, NULL, 0);
    uint8_t sec[64];
    tb_prog_t tp[] = { { 1, 0x100 } };
    size_t n = tb_pat(sec, 1, 0, tp, 1);
    sec[n - 1] ^= 0xFF;
    uint8_t pl[188];
    size_t pn = tb_psi_payload(pl, sec, n);
    tb_pkt_t ps = { .pid = 0, .pusi = true, .pcr = true, .pcr_base = 7,
                    .payload = pl, .payload_len = pn };
    tb_packet(pk, &ps);
    CHECK(moq_mpeg2ts_switcher_process(sw, pk) == MOQ_OK);
    CHECK((pk[5] & 0x80) != 0);
    CHECK(tb_crc32(pk + 13, n) != 0);

    /* First packet of another PID: the indicator when it has an adaptation
     * field; a payload-only packet is left alone and counted. */
    moq_mpeg2ts_switcher_switch(sw, NULL, 0);
    tb_pkt_t a = { .pid = 0x102, .af = true, .cc = 2 };
    tb_packet(pk, &a);
    sw_run(sw, pk, 1);
    CHECK((pk[5] & 0x80) != 0);
    tb_pkt_t a2 = { .pid = 0x102, .af = true, .cc = 3 };
    tb_packet(pk, &a2);
    sw_run(sw, pk, 1);
    CHECK((pk[5] & 0x80) == 0);                /* only the first */
    tb_pkt_t b = { .pid = 0x103, .cc = 2 };
    tb_packet(pk, &b);
    memcpy(copy, pk, 188);
    sw_run(sw, pk, 1);
    CHECK(memcmp(copy, pk, 188) == 0);
    moq_mpeg2ts_switcher_stats_t st;
    CHECK(moq_mpeg2ts_switcher_get_stats(sw, &st, sizeof(st)) == MOQ_OK);
    CHECK(st.switches == 3);
    CHECK(st.first_packet_di_skipped >= 1);
    CHECK(st.pcr_discontinuities_set == 2);
    free(sw);
}

/* -- Join alignment with a duplicate ------------------------------------- */

static void test_align_join_duplicate(void)
{
    uint8_t x[600];
    size_t xn = pmt_with_descs(x, 0, 400);
    uint8_t three[188 * 3];
    size_t pl = 0;
    uint8_t cc = 0;
    moq_mpeg2ts_packetize_section(0x100, &cc, B(x, xn), 188, three,
                                  sizeof(three), &pl);
    struct { uint8_t next_cc; bool next_payload; uint8_t want[4]; } cases[] = {
        { 7, true,  { 4, 5, 5, 6 } },
        { 1, true,  { 14, 15, 15, 0 } },
        { 5, false, { 3, 4, 4, 5 } },         /* adaptation-only next */
    };
    for (size_t c = 0; c < 3; c++) {
        uint8_t init[188 * 4];
        memcpy(init, three, 188);
        memcpy(init + 188, three + 188, 188);
        memcpy(init + 376, three + 188, 188); /* duplicate of packet 1 */
        memcpy(init + 564, three + 376, 188);
        uint8_t first[188];
        tb_pkt_t f = { .pid = 0x100, .pusi = cases[c].next_payload,
                       .no_payload = !cases[c].next_payload,
                       .cc = cases[c].next_cc };
        tb_packet(first, &f);
        CHECK(moq_mpeg2ts_align_join_psi(init, sizeof(init), 188, B(first, 188)) ==
              MOQ_OK);
        for (size_t i = 0; i < 4; i++)
            CHECK((init[i * 188 + 3] & 0x0F) == cases[c].want[i]);
        /* Reassembled, the init packets give the PMT exactly once. */
        uint8_t storage[MOQ_MPEG2TS_SECTION_MAX];
        moq_mpeg2ts_section_buf_t sb;
        moq_mpeg2ts_section_buf_init(&sb, sizeof(sb), storage, sizeof(storage));
        static got_t g;
        memset(&g, 0, sizeof(g));
        for (size_t i = 0; i < 4; i++) feed(&sb, init + i * 188, &g);
        CHECK(g.n == 1 && g.len[0] == xn && memcmp(g.sec[0], x, xn) == 0);
    }
}

/* -- Receiver --------------------------------------------------------------- */

static moq_mpeg2ts_receiver_t *new_rx(uint16_t ra_pid,
                                      moq_mpeg2ts_reorder_slot_t *slots,
                                      size_t nslots, uint64_t window)
{
    moq_mpeg2ts_receiver_cfg_t cfg;
    moq_mpeg2ts_receiver_cfg_init(&cfg);
    cfg.random_access_pid = ra_pid;
    cfg.reorder_slots = slots;
    cfg.reorder_slot_count = nslots;
    cfg.reorder_window_us = window;
    moq_mpeg2ts_receiver_t *rx = calloc(1, sizeof(*rx));
    CHECK(moq_mpeg2ts_receiver_init(rx, sizeof(*rx), &cfg) == MOQ_OK);
    return rx;
}

/* Packets of one program: PAT, PMT (video 0x101, audio 0x102), video RAP,
 * video, audio with random_access_indicator (as many muxers set it). */
static void prog_psi(uint8_t *pk2)
{
    uint8_t sec[64];
    tb_prog_t tp[] = { { 1, 0x100 } };
    size_t n = tb_pat(sec, 1, 0, tp, 1);
    psi_pkt(pk2, 0, 0, sec, n);
    tb_es_t es[] = { { 0x1B, 0x101, NULL, 0 }, { 0x0F, 0x102, NULL, 0 } };
    n = tb_pmt(sec, 1, 0, 0x101, NULL, 0, es, 2);
    psi_pkt(pk2 + 188, 0x100, 0, sec, n);
}

static void video_pkt(uint8_t *pk, bool rai, uint64_t pcr, uint8_t cc)
{
    uint8_t pes[32];
    size_t pn = tb_pes(pes, 0xE0, true, pcr + 1000, false, 0);
    tb_pkt_t v = { .pid = 0x101, .pusi = true, .rai = rai, .cc = cc,
                   .pcr = true, .pcr_base = pcr, .payload = pes,
                   .payload_len = pn };
    tb_packet(pk, &v);
}

static void audio_pkt(uint8_t *pk, uint8_t cc)
{
    uint8_t pes[32];
    size_t pn = tb_pes(pes, 0xC0, true, 5000, false, 0);
    tb_pkt_t a = { .pid = 0x102, .pusi = true, .rai = true, .cc = cc,
                   .payload = pes, .payload_len = pn };
    tb_packet(pk, &a);
}

static void test_receiver_random_access_pid(void)
{
    uint8_t o0[188 * 3], o2[188], o3[188], o4[188];
    prog_psi(o0);
    video_pkt(o0 + 376, true, 1000, 0);
    audio_pkt(o2, 0);
    video_pkt(o3, false, 4000, 1);
    video_pkt(o4, true, 7000, 2);
    moq_mpeg2ts_receive_result_t r;

    /* Default (PID_AUTO): the PMT's video PID. Audio RAI after a gap does not
     * end the discontinuity; the next video RAP does. */
    moq_mpeg2ts_receiver_t *rx = new_rx(MOQ_MPEG2TS_PID_AUTO, NULL, 0, 0);
    CHECK(moq_mpeg2ts_receiver_push(rx, 0, 0, 0, B(o0, sizeof(o0)), &r, sizeof(r)) == MOQ_OK);
    CHECK(r.random_access && !r.discontinuous);
    CHECK(moq_mpeg2ts_receiver_push(rx, 0, 2, 0, B(o2, 188), &r, sizeof(r)) == MOQ_OK);
    CHECK(r.gap && r.gap_object == 1);
    CHECK(r.discontinuous && !r.random_access);
    CHECK(moq_mpeg2ts_receiver_push(rx, 0, 3, 0, B(o3, 188), &r, sizeof(r)) == MOQ_OK);
    CHECK(r.discontinuous);
    CHECK(moq_mpeg2ts_receiver_push(rx, 0, 4, 0, B(o4, 188), &r, sizeof(r)) == MOQ_OK);
    CHECK(r.random_access && !r.discontinuous);
    free(rx);

    /* PID_NONE keeps the any-PID rule. */
    rx = new_rx(MOQ_MPEG2TS_PID_NONE, NULL, 0, 0);
    moq_mpeg2ts_receiver_push(rx, 0, 0, 0, B(o0, sizeof(o0)), &r, sizeof(r));
    CHECK(moq_mpeg2ts_receiver_push(rx, 0, 2, 0, B(o2, 188), &r, sizeof(r)) == MOQ_OK);
    CHECK(r.random_access && !r.discontinuous);
    free(rx);

    /* Before any PMT, PID_AUTO goes by the PES stream_id: audio (0xC0) does
     * not count, video (0xE0) does. */
    rx = new_rx(MOQ_MPEG2TS_PID_AUTO, NULL, 0, 0);
    CHECK(moq_mpeg2ts_receiver_push(rx, 3, 1, 0, B(o2, 188), &r, sizeof(r)) == MOQ_OK);
    CHECK(r.joined_mid_group && !r.random_access);
    CHECK(moq_mpeg2ts_receiver_push(rx, 3, 2, 0, B(o4, 188), &r, sizeof(r)) == MOQ_OK);
    CHECK(r.random_access);
    free(rx);

    /* An explicit PID. */
    rx = new_rx(0x102, NULL, 0, 0);
    CHECK(moq_mpeg2ts_receiver_push(rx, 0, 0, 0, B(o2, 188), &r, sizeof(r)) == MOQ_OK);
    CHECK(r.random_access);
    free(rx);
}

static void test_receiver_rejected_run(void)
{
    uint8_t good[188], bad[188];
    tb_pkt_t p = { .pid = 0x101 };
    tb_packet(good, &p);
    memset(bad, 0, sizeof(bad));
    moq_mpeg2ts_receive_result_t r;

    /* Rejected 1 and 2 after valid 0: the run is reported once, at 1. */
    moq_mpeg2ts_receiver_t *rx = new_rx(MOQ_MPEG2TS_PID_AUTO, NULL, 0, 0);
    CHECK(moq_mpeg2ts_receiver_push(rx, 0, 0, 0, B(good, 188), &r, sizeof(r)) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_push(rx, 0, 1, 0, B(bad, 188), &r, sizeof(r)) == MOQ_ERR_PROTO);
    CHECK(!r.gap);
    CHECK(moq_mpeg2ts_receiver_push(rx, 0, 2, 0, B(bad, 188), &r, sizeof(r)) == MOQ_ERR_PROTO);
    CHECK(!r.gap);
    CHECK(moq_mpeg2ts_receiver_push(rx, 0, 3, 0, B(good, 188), &r, sizeof(r)) == MOQ_OK);
    CHECK(r.gap && r.gap_group == 0 && r.gap_object == 1);
    CHECK(moq_mpeg2ts_receiver_push(rx, 0, 4, 0, B(good, 188), &r, sizeof(r)) == MOQ_OK);
    CHECK(!r.gap);
    free(rx);

    /* Missing 1, rejected 2 and 3: reported once, at 1, on Object 2. */
    rx = new_rx(MOQ_MPEG2TS_PID_AUTO, NULL, 0, 0);
    moq_mpeg2ts_receiver_push(rx, 0, 0, 0, B(good, 188), &r, sizeof(r));
    CHECK(moq_mpeg2ts_receiver_push(rx, 0, 2, 0, B(bad, 188), &r, sizeof(r)) == MOQ_ERR_PROTO);
    CHECK(r.gap && r.gap_object == 1);
    CHECK(moq_mpeg2ts_receiver_push(rx, 0, 3, 0, B(bad, 188), &r, sizeof(r)) == MOQ_ERR_PROTO);
    CHECK(!r.gap);
    CHECK(moq_mpeg2ts_receiver_push(rx, 0, 4, 0, B(good, 188), &r, sizeof(r)) == MOQ_OK);
    CHECK(!r.gap);
    /* A later separate loss is a new run. */
    CHECK(moq_mpeg2ts_receiver_push(rx, 0, 6, 0, B(good, 188), &r, sizeof(r)) == MOQ_OK);
    CHECK(r.gap && r.gap_object == 5);
    free(rx);
}

static void test_receiver_group_end(void)
{
    uint8_t good[188];
    tb_pkt_t p = { .pid = 0x101 };
    tb_packet(good, &p);
    moq_bytes_t G = B(good, 188);
    moq_mpeg2ts_receive_result_t r;
    const uint32_t EOG = MOQ_MPEG2TS_OBJ_SUBGROUP_ENDS_GROUP;

    /* The subgroup END_OF_GROUP bit on every Object: no false gap inside
     * the Group; without the subgroup's FIN the next Group's start reports
     * the tail as uncertain, not as a loss. */
    moq_mpeg2ts_receiver_t *rx = new_rx(MOQ_MPEG2TS_PID_AUTO, NULL, 0, 0);
    CHECK(moq_mpeg2ts_receiver_push(rx, 0, 0, EOG, G, &r, sizeof(r)) == MOQ_OK && !r.gap);
    CHECK(moq_mpeg2ts_receiver_push(rx, 0, 1, EOG, G, &r, sizeof(r)) == MOQ_OK && !r.gap);
    CHECK(moq_mpeg2ts_receiver_push(rx, 1, 0, EOG, G, &r, sizeof(r)) == MOQ_OK);
    CHECK(r.gap && r.gap_tail_unknown && r.gap_object == 2 && !r.discontinuous);
    /* Skipping a whole Group is still a gap, at its first Object. */
    CHECK(moq_mpeg2ts_receiver_push(rx, 3, 0, EOG, G, &r, sizeof(r)) == MOQ_OK);
    CHECK(r.gap && !r.gap_tail_unknown && r.gap_group == 1 && r.gap_object == 1);
    free(rx);

    /* No bit, no end signal: the next Group reports an unknown tail and the
     * stream is not made discontinuous. */
    rx = new_rx(MOQ_MPEG2TS_PID_AUTO, NULL, 0, 0);
    moq_mpeg2ts_receiver_push(rx, 0, 0, 0, G, &r, sizeof(r));
    CHECK(moq_mpeg2ts_receiver_push(rx, 1, 0, 0, G, &r, sizeof(r)) == MOQ_OK);
    CHECK(r.gap && r.gap_tail_unknown && r.gap_object == 1 && !r.discontinuous);
    /* end_group after the subgroup's FIN: no gap. */
    moq_mpeg2ts_receiver_end_group(rx, 1);
    CHECK(moq_mpeg2ts_receiver_push(rx, 2, 0, 0, G, &r, sizeof(r)) == MOQ_OK);
    CHECK(!r.gap && !r.gap_tail_unknown);
    /* A known end that was not reached is a real gap. */
    moq_mpeg2ts_receiver_push(rx, 2, 1, 0, G, &r, sizeof(r));
    moq_mpeg2ts_receiver_end_group(rx, 2);
    CHECK(moq_mpeg2ts_receiver_push(rx, 3, 0, 0, G, &r, sizeof(r)) == MOQ_OK && !r.gap);
    free(rx);
}

static void test_receiver_reorder(void)
{
    uint8_t good[188];
    tb_pkt_t p = { .pid = 0x101 };
    tb_packet(good, &p);
    moq_bytes_t G = B(good, 188);
    const uint32_t EOG = MOQ_MPEG2TS_OBJ_SUBGROUP_ENDS_GROUP;
    moq_mpeg2ts_reorder_slot_t slots[4];
    moq_mpeg2ts_delivery_t d;

    moq_mpeg2ts_receiver_t *rx = new_rx(MOQ_MPEG2TS_PID_AUTO, slots, 4, 100);
    int tag[16];
    CHECK(moq_mpeg2ts_receiver_offer(rx, 0, 2, 0, EOG, G, &tag[0]) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 0, &d, sizeof(d)) == MOQ_OK);       /* a Group start */
    CHECK(d.group == 2 && d.object == 0 && d.ctx == &tag[0]);
    CHECK(d.struct_size == sizeof(d) && d.result.struct_size == sizeof(d.result));
    CHECK(d.result.accepted && d.result.joined_mid_group == false);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 0, &d, sizeof(d)) == MOQ_DONE);

    /* The next Group's first Object overtakes the end of this one. */
    CHECK(moq_mpeg2ts_receiver_offer(rx, 5, 3, 0, EOG, G, &tag[1]) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 5, &d, sizeof(d)) == MOQ_DONE);
    CHECK(moq_mpeg2ts_receiver_next_deadline_us(rx) == 105);
    CHECK(moq_mpeg2ts_receiver_offer(rx, 6, 2, 1, EOG, G, &tag[2]) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 6, &d, sizeof(d)) == MOQ_OK);
    CHECK(d.group == 2 && d.object == 1 && !d.result.gap);
    /* (2, 1) may not be the last: (3, 0) waits for the window. */
    CHECK(moq_mpeg2ts_receiver_poll(rx, 104, &d, sizeof(d)) == MOQ_DONE);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 105, &d, sizeof(d)) == MOQ_OK);
    CHECK(d.group == 3 && d.object == 0);
    CHECK(d.result.gap && d.result.gap_tail_unknown && d.result.gap_object == 2);

    /* Late and duplicate offers are refused. */
    CHECK(moq_mpeg2ts_receiver_offer(rx, 106, 2, 2, EOG, G, NULL) == MOQ_ERR_PROTO);
    CHECK(moq_mpeg2ts_receiver_offer(rx, 106, 3, 2, EOG, G, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_offer(rx, 106, 3, 2, EOG, G, NULL) == MOQ_ERR_PROTO);
    CHECK(moq_mpeg2ts_receiver_offer(rx, 106, 3, 1, EOG, G, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 106, &d, sizeof(d)) == MOQ_OK && d.object == 1);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 106, &d, sizeof(d)) == MOQ_OK && d.object == 2);
    CHECK(moq_mpeg2ts_receiver_offer(rx, 106, 3, 1, EOG, G, NULL) == MOQ_ERR_PROTO);

    /* The subgroup's FIN (end_group) releases the next Group at once. */
    CHECK(moq_mpeg2ts_receiver_offer(rx, 107, 4, 0, EOG, G, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 107, &d, sizeof(d)) == MOQ_DONE);   /* (3, 3) could follow */
    moq_mpeg2ts_receiver_end_group(rx, 3);            /* final is (3, 2) */
    CHECK(moq_mpeg2ts_receiver_poll(rx, 107, &d, sizeof(d)) == MOQ_OK);
    CHECK(d.group == 4 && d.object == 0 && !d.result.gap &&
          !d.result.gap_tail_unknown);

    /* LAST_IN_GROUP on the offer does the same. */
    CHECK(moq_mpeg2ts_receiver_offer(rx, 108, 5, 0, EOG, G, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_offer(rx, 108, 4, 1,
                                     EOG | MOQ_MPEG2TS_OBJ_LAST_IN_GROUP, G,
                                     NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 108, &d, sizeof(d)) == MOQ_OK && d.group == 4 && d.object == 1);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 108, &d, sizeof(d)) == MOQ_OK && d.group == 5 && !d.result.gap);

    /* Every slot in use: the first held Object is due. */
    for (uint64_t o = 2; o < 6; o++)
        CHECK(moq_mpeg2ts_receiver_offer(rx, 200, 5, o, EOG, G, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_offer(rx, 200, 5, 7, EOG, G, NULL) == MOQ_ERR_BUFFER);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 200, &d, sizeof(d)) == MOQ_OK && d.object == 2 && d.result.gap);
    for (int i = 0; i < 3; i++) CHECK(moq_mpeg2ts_receiver_poll(rx, 200, &d, sizeof(d)) == MOQ_OK);
    CHECK(d.object == 5);
    CHECK(moq_mpeg2ts_receiver_next_deadline_us(rx) == UINT64_MAX);

    /* finish() releases what is held, in order. */
    CHECK(moq_mpeg2ts_receiver_offer(rx, 300, 6, 3, EOG, G, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_offer(rx, 300, 6, 1, EOG, G, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 300, &d, sizeof(d)) == MOQ_DONE);
    moq_mpeg2ts_receiver_finish(rx);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 300, &d, sizeof(d)) == MOQ_OK && d.group == 6 && d.object == 1);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 300, &d, sizeof(d)) == MOQ_OK && d.object == 3);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 300, &d, sizeof(d)) == MOQ_DONE);
    free(rx);

    /* No slots: offer is refused. */
    rx = new_rx(MOQ_MPEG2TS_PID_AUTO, NULL, 0, 0);
    CHECK(moq_mpeg2ts_receiver_offer(rx, 0, 0, 0, 0, G, NULL) == MOQ_ERR_INVAL);
    free(rx);
}

/* MOQ_MPEG2TS_OBJ_JOINABLE: a known mid-Group join starts the window after
 * the join grace, not after the whole window; Object 0, a swapped first pair
 * and a started window keep today's rules. Window 300 ms, grace 50 ms. */
static void test_receiver_join_grace(void)
{
    uint8_t good[188];
    tb_pkt_t p = { .pid = 0x101 };
    tb_packet(good, &p);
    moq_bytes_t G = B(good, 188);
    const uint32_t J = MOQ_MPEG2TS_OBJ_JOINABLE;
    const uint64_t W = 300000, GR = MOQ_MPEG2TS_JOIN_GRACE_US;
    moq_mpeg2ts_reorder_slot_t slots[4];
    moq_mpeg2ts_delivery_t d;
    CHECK(GR < W);

    /* Not joinable: a mid-Group head waits the whole window (unchanged). */
    moq_mpeg2ts_receiver_t *rx = new_rx(MOQ_MPEG2TS_PID_AUTO, slots, 4, W);
    CHECK(moq_mpeg2ts_receiver_offer(rx, 1000, 7, 12, 0, G, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_next_deadline_us(rx) == 1000 + W);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 1000 + GR, &d, sizeof(d)) == MOQ_DONE);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 1000 + W, &d, sizeof(d)) == MOQ_OK && d.object == 12);
    free(rx);

    /* Joinable mid-Group head: due after the grace, no gap, joined mid-Group. */
    rx = new_rx(MOQ_MPEG2TS_PID_AUTO, slots, 4, W);
    CHECK(moq_mpeg2ts_receiver_offer(rx, 1000, 7, 12, J, G, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_next_deadline_us(rx) == 1000 + GR);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 1000 + GR - 1, &d, sizeof(d)) == MOQ_DONE);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 1000 + GR, &d, sizeof(d)) == MOQ_OK);
    CHECK(d.group == 7 && d.object == 12 && d.result.joined_mid_group && !d.result.gap);
    /* Started: the flag no longer shortens anything. A hole waits the window. */
    CHECK(moq_mpeg2ts_receiver_offer(rx, 2000, 7, 14, J, G, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_next_deadline_us(rx) == 2000 + W);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 2000 + GR, &d, sizeof(d)) == MOQ_DONE);
    CHECK(moq_mpeg2ts_receiver_offer(rx, 2010, 7, 13, J, G, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 2010, &d, sizeof(d)) == MOQ_OK && d.object == 13);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 2010, &d, sizeof(d)) == MOQ_OK && d.object == 14);
    free(rx);

    /* A swapped first pair inside the grace goes out in order. */
    rx = new_rx(MOQ_MPEG2TS_PID_AUTO, slots, 4, W);
    CHECK(moq_mpeg2ts_receiver_offer(rx, 1000, 7, 13, J, G, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_offer(rx, 1020, 7, 12, J, G, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 1020, &d, sizeof(d)) == MOQ_DONE);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 1000 + GR, &d, sizeof(d)) == MOQ_OK && d.object == 12);
    CHECK(!d.result.gap);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 1000 + GR, &d, sizeof(d)) == MOQ_OK && d.object == 13);
    CHECK(!d.result.gap);
    free(rx);

    /* Object 0 is released at once, joinable or not (unchanged). */
    rx = new_rx(MOQ_MPEG2TS_PID_AUTO, slots, 4, W);
    CHECK(moq_mpeg2ts_receiver_offer(rx, 1000, 8, 0, J, G, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_poll(rx, 1000, &d, sizeof(d)) == MOQ_OK);
    CHECK(d.group == 8 && d.object == 0 && !d.result.joined_mid_group);
    free(rx);

    /* A window shorter than the grace bounds it. */
    rx = new_rx(MOQ_MPEG2TS_PID_AUTO, slots, 4, 20000);
    CHECK(moq_mpeg2ts_receiver_offer(rx, 1000, 7, 12, J, G, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_next_deadline_us(rx) == 1000 + 20000);
    free(rx);
}

/* -- Packetizer --------------------------------------------------------------- */

typedef struct emitted {
    size_t   count;
    uint64_t group[256];
    uint64_t object[256];
    bool     rap[256];
    size_t   packets[256];
    uint8_t  bytes[188 * 512];
    size_t   total;
} emitted_t;

static void on_emit(void *ctx, const moq_mpeg2ts_emitted_object_t *o)
{
    emitted_t *e = ctx;
    if (e->count >= 256 || e->total + o->payload.len > sizeof(e->bytes)) {
        fprintf(stderr, "FAIL: emit overflow\n");
        exit(1);
    }
    size_t i = e->count++;
    e->group[i] = o->group;
    e->object[i] = o->object;
    e->rap[i] = o->random_access;
    e->packets[i] = o->packet_count;
    memcpy(e->bytes + e->total, o->payload.data, o->payload.len);
    e->total += o->payload.len;
}

static void test_packetizer_lead_in(void)
{
    /* Lead-in runs of 1, 2 and 3 packets before each random access point,
     * with Objects of 1 to 4 packets: the point is always in its Group's
     * first Object, no Object exceeds the bound, and no packet after the
     * first Group start is lost or split. */
    for (size_t lead = 1; lead <= 3; lead++)
    for (uint32_t op = 1; op <= 4; op++) {
        uint8_t stream[188 * 40];
        size_t np = 0;
        size_t first_lead = 0;
        for (int gop = 0; gop < 3; gop++) {
            uint8_t psi[188 * 2];
            prog_psi(psi);
            if (gop == 0) first_lead = np;
            memcpy(stream + np++ * 188, psi, 188);
            if (lead >= 2) memcpy(stream + np++ * 188, psi + 188, 188);
            if (lead >= 3) {
                tb_pkt_t sdt = { .pid = 0x11, .pusi = true };
                tb_packet(stream + np++ * 188, &sdt);
            }
            video_pkt(stream + np++ * 188, true, 1000 + (uint64_t)gop * 90000,
                      (uint8_t)gop);
            for (int k = 0; k < 3; k++) {
                tb_pkt_t c = { .pid = 0x101, .cc = (uint8_t)k };
                tb_packet(stream + np++ * 188, &c);
            }
        }
        static emitted_t e;
        memset(&e, 0, sizeof(e));
        uint8_t obj[188 * 4], lb[188 * 4];
        moq_mpeg2ts_packetizer_cfg_t cfg;
        moq_mpeg2ts_packetizer_cfg_init(&cfg);
        cfg.object_packets = op;
        cfg.object_buf = obj;
        cfg.object_cap = sizeof(obj);
        cfg.lead_buf = lb;
        cfg.lead_cap = sizeof(lb);
        cfg.emit = on_emit;
        cfg.ctx = &e;
        moq_mpeg2ts_packetizer_t *pz = calloc(1, sizeof(*pz));
        CHECK(moq_mpeg2ts_packetizer_init(pz, sizeof(*pz), &cfg) == MOQ_OK);
        for (size_t i = 0; i < np; i++)
            CHECK(moq_mpeg2ts_packetizer_push(pz, B(stream + i * 188, 188)) ==
                  MOQ_OK);
        CHECK(moq_mpeg2ts_packetizer_flush(pz) == MOQ_OK);
        size_t groups = 0;
        for (size_t i = 0; i < e.count; i++) {
            CHECK(e.packets[i] >= 1 && e.packets[i] <= op);
            if (e.object[i] == 0) {
                groups++;
                CHECK(e.rap[i]);
            }
        }
        CHECK(groups == 3);
        /* Everything from the first Group's lead-in that fits, or from its
         * random access point, onwards. */
        /* A run longer than op - 1 packets cannot open the first Group:
         * it is dropped with the rest of what precedes that Group. */
        size_t start = lead < op ? first_lead : first_lead + lead;
        CHECK(e.total == (np - start) * 188);
        CHECK(memcmp(e.bytes, stream + start * 188, e.total) == 0);
        free(pz);
    }
}

static void test_packetizer_rai_before_pes(void)
{
    /* random_access_indicator on an adaptation-only packet (with the PCR)
     * announces the PES that starts in the next packet of that PID. */
    for (int mode = 0; mode < 3; mode++) {
        uint16_t sel = mode == 0 ? 0x101 : mode == 1 ? MOQ_MPEG2TS_PID_AUTO
                                                     : 0x102;
        uint8_t stream[188 * 6];
        tb_pkt_t ann = { .pid = 0x101, .no_payload = true, .rai = true,
                         .pcr = true, .pcr_base = 100 };
        tb_packet(stream, &ann);
        uint8_t pes[32];
        size_t pn = tb_pes(pes, 0xE0, true, 9000, false, 0);
        tb_pkt_t v = { .pid = 0x101, .pusi = true, .payload = pes,
                       .payload_len = pn };
        tb_packet(stream + 188, &v);
        tb_pkt_t c = { .pid = 0x101, .cc = 1 };
        tb_packet(stream + 376, &c);
        /* An audio announcement and audio PES. */
        tb_pkt_t aann = { .pid = 0x102, .no_payload = true, .rai = true };
        tb_packet(stream + 564, &aann);
        pn = tb_pes(pes, 0xC0, true, 9100, false, 0);
        tb_pkt_t a = { .pid = 0x102, .pusi = true, .payload = pes,
                       .payload_len = pn };
        tb_packet(stream + 752, &a);

        static emitted_t e;
        memset(&e, 0, sizeof(e));
        uint8_t obj[188 * 8];
        moq_mpeg2ts_packetizer_cfg_t cfg;
        moq_mpeg2ts_packetizer_cfg_init(&cfg);
        cfg.object_packets = 8;
        cfg.random_access_pid = sel;
        cfg.object_buf = obj;
        cfg.object_cap = sizeof(obj);
        cfg.emit = on_emit;
        cfg.ctx = &e;
        moq_mpeg2ts_packetizer_t *pz = calloc(1, sizeof(*pz));
        CHECK(moq_mpeg2ts_packetizer_init(pz, sizeof(*pz), &cfg) == MOQ_OK);
        for (size_t i = 0; i < 5; i++)
            moq_mpeg2ts_packetizer_push(pz, B(stream + i * 188, 188));
        moq_mpeg2ts_packetizer_flush(pz);
        moq_mpeg2ts_packetizer_stats_t st;
        CHECK(moq_mpeg2ts_packetizer_get_stats(pz, &st, sizeof(st)) == MOQ_OK);
        if (mode < 2) {
            /* The Group opens at the PES start of the video. */
            CHECK(st.groups == 1 && e.count == 1 && e.rap[0]);
            CHECK(memcmp(e.bytes, stream + 188, 188) == 0);
            CHECK(st.packets_dropped_before_random_access == 1);
        } else {
            CHECK(st.groups == 1);
            CHECK(memcmp(e.bytes, stream + 752, 188) == 0);
        }

        /* The same through inspection of one Object. */
        moq_mpeg2ts_inspect_cfg_t ip;
        moq_mpeg2ts_inspect_cfg_init(&ip);
        ip.random_access_pid = sel;
        moq_mpeg2ts_object_info_t info;
        CHECK(moq_mpeg2ts_inspect_object(&ip, B(stream, 188 * 5), &info, sizeof(info)) == MOQ_OK);
        CHECK(info.has_random_access);
        CHECK(info.random_access_index == (mode < 2 ? 1u : 4u));
        free(pz);
    }
}

/* -- A switch across two renditions -------------------------------------- */

typedef struct rendition {
    uint8_t  pk[188 * 64];
    size_t   n;
} rendition_t;

/* Groups of: PAT, PMT, video RAP with PCR, two continuation packets. */
static void make_rendition(rendition_t *r, uint16_t pmt_pid, uint16_t vpid,
                           uint64_t pcr0, uint8_t version)
{
    r->n = 0;
    uint8_t cc_pat = 0, cc_pmt = 0, cc_v = 0;
    for (int g = 0; g < 6; g++) {
        uint8_t sec[64];
        tb_prog_t tp[] = { { 1, pmt_pid } };
        size_t n = tb_pat(sec, 1, version, tp, 1);
        psi_pkt(r->pk + r->n++ * 188, 0, cc_pat++, sec, n);
        tb_es_t es[] = { { 0x1B, vpid, NULL, 0 } };
        n = tb_pmt(sec, 1, version, vpid, NULL, 0, es, 1);
        psi_pkt(r->pk + r->n++ * 188, pmt_pid, cc_pmt++, sec, n);
        uint8_t pes[32];
        size_t pn = tb_pes(pes, 0xE0, true, pcr0 + (uint64_t)g * 3000, false, 0);
        tb_pkt_t v = { .pid = vpid, .pusi = true, .rai = true, .pcr = true,
                       .pcr_base = pcr0 + (uint64_t)g * 3000, .cc = cc_v++,
                       .payload = pes, .payload_len = pn };
        tb_packet(r->pk + r->n++ * 188, &v);
        for (int k = 0; k < 2; k++) {
            tb_pkt_t c = { .pid = vpid, .cc = cc_v++ };
            cc_v &= 0x0F;
            tb_packet(r->pk + r->n++ * 188, &c);
        }
        cc_pat &= 0x0F;
        cc_pmt &= 0x0F;
    }
}

static void test_switch_renditions(void)
{
    static rendition_t a, b;
    make_rendition(&a, 0x100, 0x101, 1000, 0);
    make_rendition(&b, 0x1000, 0x1011, 500000, 0);
    static uint8_t out[188 * 64];
    size_t n = 0;
    moq_mpeg2ts_switcher_t *sw = new_switcher();
    /* Groups 0..2 from A, 3..5 from B; a Group is 5 packets. */
    for (size_t i = 0; i < 15; i++) memcpy(out + n++ * 188, a.pk + i * 188, 188);
    size_t switch_at = n;
    for (size_t i = 15; i < 30; i++) memcpy(out + n++ * 188, b.pk + i * 188, 188);
    for (size_t i = 0; i < n; i++) {
        if (i == switch_at) moq_mpeg2ts_switcher_switch(sw, NULL, 0);
        CHECK(moq_mpeg2ts_switcher_process(sw, out + i * 188) == MOQ_OK);
    }
    /* What a receiver sees: every PSI section valid; versions change once,
     * at the switch, and stay put after it; the first PCR after the switch
     * has the discontinuity_indicator, later ones do not. */
    int pat_v = -1, pmt_v = -1, pat_changes = 0, pmt_changes = 0;
    bool first_pcr_di = false;
    int pcr_seen_after = 0, di_after = 0;
    uint16_t pmt_pid = 0x100;
    for (size_t i = 0; i < n; i++) {
        moq_mpeg2ts_packet_t p;
        moq_mpeg2ts_packet_init(&p);
        CHECK(moq_mpeg2ts_parse_packet(B(out + i * 188, 188), 188, &p) == MOQ_OK);
        if (p.pid == 0 || p.pid == pmt_pid) {
            const uint8_t *s = p.payload.data + 1;
            size_t len = 3u + (((size_t)(s[1] & 0x0F) << 8) | s[2]);
            CHECK(tb_crc32(s, len) == 0);
            int v = (s[5] >> 1) & 0x1F;
            if (p.pid == 0) {
                if (pat_v >= 0 && v != pat_v) pat_changes++;
                pat_v = v;
                pmt_pid = (uint16_t)(((s[10] & 0x1F) << 8) | s[11]);
            } else {
                if (pmt_v >= 0 && v != pmt_v) pmt_changes++;
                pmt_v = v;
            }
        }
        if (p.has_pcr && i >= switch_at) {
            if (pcr_seen_after == 0) first_pcr_di = p.discontinuity;
            else if (p.discontinuity) di_after++;
            pcr_seen_after++;
        }
    }
    CHECK(pat_changes == 1 && pmt_changes == 1);
    CHECK(pat_v == 1 && pmt_v == 1);
    CHECK(first_pcr_di && di_after == 0);
    free(sw);
}


/* -- Receiver, reassembler and switcher boundaries ------------------------ */

/* PCR_PID of the PMT section carried after pointer_field 0. */
static uint16_t rd_pmt_pcr(const uint8_t *pkt)
{
    const uint8_t *s = pkt + 5;
    size_t len = 3u + (((size_t)(s[1] & 0x0F) << 8) | s[2]);
    if (tb_crc32(s, len) != 0) return 0xFFFF;
    return (uint16_t)(((s[8] & 0x1F) << 8) | s[9]);
}

/* version_number of the PSI section carried after pointer_field 0, or
 * 0xFF when its CRC_32 does not check. */
static uint8_t rd_psi_version(const uint8_t *pkt)
{
    const uint8_t *s = pkt + 5;
    size_t len = 3u + (((size_t)(s[1] & 0x0F) << 8) | s[2]);
    if (tb_crc32(s, len) != 0) return 0xFF;
    return (uint8_t)((s[5] >> 1) & 0x1F);
}

static void new_window_rx(moq_mpeg2ts_receiver_t *rx,
                          moq_mpeg2ts_reorder_slot_t slots[4])
{
    moq_mpeg2ts_receiver_cfg_t cfg;
    moq_mpeg2ts_receiver_cfg_init(&cfg);
    cfg.reorder_slots = slots;
    cfg.reorder_slot_count = 4;
    cfg.reorder_window_us = 100;
    CHECK(moq_mpeg2ts_receiver_init(rx, sizeof(*rx), &cfg) == MOQ_OK);
}

/* An END_OF_GROUP status Object names the first Object ID that does not
 * exist (D16 10.2.1.1, D18 11.2.1.1): end_group_at(g, N) makes N - 1 the
 * final Object, so a missing (g, N - 1) stays a known gap, and N = 0 marks
 * the Group empty, so the next one follows it without a gap. */
static void test_receiver_end_group_at(void)
{
    uint8_t pkt[188];
    const uint8_t data[] = { 1, 2, 3 };
    tb_pkt_t p = { .pid = 0x101, .payload = data, .payload_len = sizeof(data) };
    tb_packet(pkt, &p);
    moq_bytes_t P = B(pkt, sizeof(pkt));
    static moq_mpeg2ts_receiver_t rx;
    moq_mpeg2ts_reorder_slot_t slots[4];
    moq_mpeg2ts_delivery_t d;

    /* (0, 1) missing: the next Group waits for it, then reports it lost. */
    new_window_rx(&rx, slots);
    CHECK(moq_mpeg2ts_receiver_offer(&rx, 0, 0, 0, 0, P, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, 0, &d, sizeof(d)) == MOQ_OK);
    moq_mpeg2ts_receiver_end_group_at(&rx, 0, 2);
    CHECK(moq_mpeg2ts_receiver_offer(&rx, 1, 1, 0, 0, P, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, 1, &d, sizeof(d)) == MOQ_DONE);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, 101, &d, sizeof(d)) == MOQ_OK && d.group == 1);
    CHECK(d.result.gap && !d.result.gap_tail_unknown);
    CHECK(d.result.gap_group == 0 && d.result.gap_object == 1);

    /* The final Object present: the next Group is released at once. */
    new_window_rx(&rx, slots);
    moq_mpeg2ts_receiver_offer(&rx, 0, 0, 0, 0, P, NULL);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, 0, &d, sizeof(d)) == MOQ_OK);
    moq_mpeg2ts_receiver_end_group_at(&rx, 0, 1);
    moq_mpeg2ts_receiver_offer(&rx, 1, 1, 0, 0, P, NULL);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, 1, &d, sizeof(d)) == MOQ_OK && d.group == 1);
    CHECK(!d.result.gap && !d.result.gap_tail_unknown);

    /* An empty Group is stepped over. */
    moq_mpeg2ts_receiver_end_group_at(&rx, 1, 1);
    moq_mpeg2ts_receiver_end_group_at(&rx, 2, 0);
    moq_mpeg2ts_receiver_end_group_at(&rx, 3, 0);
    moq_mpeg2ts_receiver_offer(&rx, 2, 4, 0, 0, P, NULL);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, 2, &d, sizeof(d)) == MOQ_OK && d.group == 4);
    CHECK(!d.result.gap && !d.result.gap_tail_unknown);
}

/* The END_OF_GROUP bit names the subgroup holding the Group's largest
 * Object; only that subgroup's FIN says which Object it is (D16 10.4.2, D18
 * 11.4.2). Without the FIN, moving on keeps the tail uncertain. */
static void test_receiver_end_bit_needs_fin(void)
{
    uint8_t pkt[188];
    const uint8_t data[] = { 1, 2, 3 };
    tb_pkt_t p = { .pid = 0x101, .payload = data, .payload_len = sizeof(data) };
    tb_packet(pkt, &p);
    moq_bytes_t P = B(pkt, sizeof(pkt));
    const uint32_t EOG = MOQ_MPEG2TS_OBJ_SUBGROUP_ENDS_GROUP;
    static moq_mpeg2ts_receiver_t rx;
    moq_mpeg2ts_reorder_slot_t slots[4];
    moq_mpeg2ts_delivery_t d;
    moq_mpeg2ts_receive_result_t r;

    /* Window expiry. */
    new_window_rx(&rx, slots);
    CHECK(moq_mpeg2ts_receiver_offer(&rx, 0, 0, 0, EOG, P, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, 0, &d, sizeof(d)) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_offer(&rx, 0, 1, 0, 0, P, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, 100, &d, sizeof(d)) == MOQ_OK);
    CHECK(d.result.gap && d.result.gap_tail_unknown);
    CHECK(d.result.gap_group == 0 && d.result.gap_object == 1);
    CHECK(moq_mpeg2ts_receiver_offer(&rx, 101, 0, 1, 0, P, NULL) == MOQ_ERR_PROTO);

    /* A delayed tail that arrives inside the window is delivered in order. */
    new_window_rx(&rx, slots);
    moq_mpeg2ts_receiver_offer(&rx, 0, 0, 0, EOG, P, NULL);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, 0, &d, sizeof(d)) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_offer(&rx, 10, 1, 0, EOG, P, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, 50, &d, sizeof(d)) == MOQ_DONE);
    CHECK(moq_mpeg2ts_receiver_offer(&rx, 60, 0, 1, EOG, P, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, 60, &d, sizeof(d)) == MOQ_OK && d.object == 1 && !d.result.gap);
    /* Then the subgroup's FIN: the next Group is released at once. */
    moq_mpeg2ts_receiver_end_group(&rx, 0);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, 60, &d, sizeof(d)) == MOQ_OK && d.group == 1);
    CHECK(!d.result.gap && !d.result.gap_tail_unknown);

    /* A reset subgroup gives no FIN: release by a full window, or by
     * finish(), still reports the tail as uncertain. */
    new_window_rx(&rx, slots);
    moq_mpeg2ts_receiver_offer(&rx, 0, 0, 0, EOG, P, NULL);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, 0, &d, sizeof(d)) == MOQ_OK);
    for (uint64_t o = 0; o < 4; o++)
        CHECK(moq_mpeg2ts_receiver_offer(&rx, 1, 1, o, EOG, P, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, 1, &d, sizeof(d)) == MOQ_OK && d.group == 1 && d.object == 0);
    CHECK(d.result.gap && d.result.gap_tail_unknown);
    new_window_rx(&rx, slots);
    moq_mpeg2ts_receiver_offer(&rx, 0, 0, 0, EOG, P, NULL);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, 0, &d, sizeof(d)) == MOQ_OK);
    moq_mpeg2ts_receiver_offer(&rx, 1, 1, 0, EOG, P, NULL);
    moq_mpeg2ts_receiver_finish(&rx);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, 1, &d, sizeof(d)) == MOQ_OK && d.group == 1);
    CHECK(d.result.gap && d.result.gap_tail_unknown);

    /* The same through push. */
    new_window_rx(&rx, slots);
    CHECK(moq_mpeg2ts_receiver_push(&rx, 0, 0, EOG, P, &r, sizeof(r)) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_push(&rx, 1, 0, EOG, P, &r, sizeof(r)) == MOQ_OK);
    CHECK(r.gap && r.gap_tail_unknown && !r.discontinuous);
}

/* The deadline poll() is advertised to keep is one at which poll() makes
 * progress, whatever the arrival order. */
static void test_receiver_deadline_progress(void)
{
    uint8_t pkt[188];
    tb_pkt_t p = { .pid = 0x101 };
    tb_packet(pkt, &p);
    moq_bytes_t P = B(pkt, sizeof(pkt));
    static moq_mpeg2ts_receiver_t rx;
    moq_mpeg2ts_reorder_slot_t slots[4];
    moq_mpeg2ts_delivery_t d;
    moq_mpeg2ts_receive_result_t r;

    new_window_rx(&rx, slots);
    CHECK(moq_mpeg2ts_receiver_push(&rx, 0, 0, 0, P, &r, sizeof(r)) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_offer(&rx, 0, 0, 3, 0, P, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_offer(&rx, 50, 0, 2, 0, P, NULL) == MOQ_OK);
    uint64_t deadline = moq_mpeg2ts_receiver_next_deadline_us(&rx);
    CHECK(deadline == 100);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, deadline - 1, &d, sizeof(d)) == MOQ_DONE);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, deadline, &d, sizeof(d)) == MOQ_OK);
    CHECK(d.object == 2 && d.result.gap && d.result.gap_object == 1);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, deadline, &d, sizeof(d)) == MOQ_OK && d.object == 3);
    CHECK(moq_mpeg2ts_receiver_next_deadline_us(&rx) == UINT64_MAX);

    /* At the clock limit the deadline saturates and is still honoured. */
    new_window_rx(&rx, slots);
    CHECK(moq_mpeg2ts_receiver_push(&rx, 0, 0, 0, P, &r, sizeof(r)) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_offer(&rx, UINT64_MAX - 10, 0, 5, 0, P, NULL) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_next_deadline_us(&rx) == UINT64_MAX);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, UINT64_MAX - 1, &d, sizeof(d)) == MOQ_DONE);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, UINT64_MAX, &d, sizeof(d)) == MOQ_OK && d.object == 5);
}

/* A rejected Object is a known loss even when the next Object opens the
 * next Group without an end signal. */
static void test_receiver_known_loss_across_groups(void)
{
    uint8_t pkt[188];
    const uint8_t data[] = { 1, 2, 3 };
    tb_pkt_t p = { .pid = 0x101, .payload = data, .payload_len = sizeof(data) };
    tb_packet(pkt, &p);
    static moq_mpeg2ts_receiver_t rx;
    moq_mpeg2ts_reorder_slot_t slots[4];
    moq_mpeg2ts_receive_result_t r;

    new_window_rx(&rx, slots);
    CHECK(moq_mpeg2ts_receiver_push(&rx, 0, 0, 0, B(pkt, sizeof(pkt)), &r, sizeof(r)) == MOQ_OK);
    CHECK(moq_mpeg2ts_receiver_push(&rx, 0, 1, 0, B(pkt, 1), &r, sizeof(r)) == MOQ_ERR_PROTO);
    CHECK(moq_mpeg2ts_receiver_push(&rx, 1, 0, 0, B(pkt, sizeof(pkt)), &r, sizeof(r)) == MOQ_OK);
    CHECK(r.gap && !r.gap_tail_unknown && r.gap_group == 0 && r.gap_object == 1);
    CHECK(r.discontinuous);

    /* Control: no rejection, the same move is an uncertain tail. */
    new_window_rx(&rx, slots);
    moq_mpeg2ts_receiver_push(&rx, 0, 0, 0, B(pkt, sizeof(pkt)), &r, sizeof(r));
    CHECK(moq_mpeg2ts_receiver_push(&rx, 1, 0, 0, B(pkt, sizeof(pkt)), &r, sizeof(r)) == MOQ_OK);
    CHECK(r.gap && r.gap_tail_unknown && !r.discontinuous);
}

/* Two different CRC-valid PSI sections can give equal payload CRCs; only
 * equal bytes make a duplicate (2.4.3.3). */
static void test_duplicates_are_byte_equal(void)
{
    uint8_t sec_a[64], sec_b[64], pkt_a[188], pkt_b[188];
    tb_prog_t progs_a[] = { { 1, 0x100 } };
    tb_prog_t progs_b[] = { { 1, 0x200 } };
    size_t na = tb_pat(sec_a, 1, 0, progs_a, 1);
    size_t nb = tb_pat(sec_b, 1, 1, progs_b, 1);
    psi_pkt(pkt_a, 0, 0, sec_a, na);
    psi_pkt(pkt_b, 0, 0, sec_b, nb);
    moq_mpeg2ts_packet_t pa, pb;
    CHECK(moq_mpeg2ts_parse_packet(B(pkt_a, 188), 188, &pa) == MOQ_OK);
    CHECK(moq_mpeg2ts_parse_packet(B(pkt_b, 188), 188, &pb) == MOQ_OK);
    uint32_t ca = tb_crc32(pa.payload.data, pa.payload.len);
    uint32_t cb = tb_crc32(pb.payload.data, pb.payload.len);
    CHECK(ca == cb && memcmp(pkt_a, pkt_b, sizeof(pkt_a)) != 0);

    uint8_t storage[MOQ_MPEG2TS_SECTION_MAX];
    moq_mpeg2ts_section_buf_t sb;
    CHECK(moq_mpeg2ts_section_buf_init(&sb, sizeof(sb), storage, sizeof(storage)) ==
          MOQ_OK);
    static got_t g;
    memset(&g, 0, sizeof(g));
    feed(&sb, pkt_a, &g);
    CHECK(g.n == 1);
    feed(&sb, pkt_b, &g);
    CHECK(g.n == 2 && g.len[1] == nb && memcmp(g.sec[1], sec_b, nb) == 0);
    feed(&sb, pkt_b, &g);                 /* an exact duplicate */
    CHECK(g.n == 2);

    moq_mpeg2ts_switcher_t *sw = new_switcher();
    CHECK(moq_mpeg2ts_switcher_process(sw, pkt_a) == MOQ_OK);
    CHECK(moq_mpeg2ts_switcher_process(sw, pkt_b) == MOQ_OK);
    CHECK(moq_mpeg2ts_parse_packet(B(pkt_b, 188), 188, &pb) == MOQ_OK);
    moq_mpeg2ts_program_t program[1];
    moq_mpeg2ts_pat_t pat;
    moq_mpeg2ts_pat_init(&pat, program, 1);
    CHECK(moq_mpeg2ts_parse_pat(B(pb.payload.data + 1, nb), &pat) == MOQ_OK);
    CHECK(program[0].pmt_pid == 0x200);
    free(sw);

    /* The same for a PMT whose PCR PID and stream change. */
    tb_es_t ea[] = { { 0x1B, 0x101, NULL, 0 } };
    tb_es_t eb[] = { { 0x1B, 0x102, NULL, 0 } };
    na = tb_pmt(sec_a, 1, 0, 0x101, NULL, 0, ea, 1);
    nb = tb_pmt(sec_b, 1, 0, 0x102, NULL, 0, eb, 1);
    psi_pkt(pkt_a, 0x100, 3, sec_a, na);
    psi_pkt(pkt_b, 0x100, 3, sec_b, nb);
    CHECK(memcmp(pkt_a, pkt_b, 188) != 0);
    moq_mpeg2ts_section_buf_reset(&sb);
    memset(&g, 0, sizeof(g));
    feed(&sb, pkt_a, &g);
    feed(&sb, pkt_b, &g);
    CHECK(g.n == 2 && g.len[1] == nb && memcmp(g.sec[1], sec_b, nb) == 0);
    /* After a switch from an output PMT of version 0, both are rewritten
     * to version 1: the second is a new section, not a replay of the
     * first's rewrite. */
    sw = new_switcher();
    sw_track_a(sw, 0, 0, 0x100);
    moq_mpeg2ts_switcher_switch(sw, (const uint16_t[]){ 0x100 }, 1);
    CHECK(moq_mpeg2ts_switcher_process(sw, pkt_a) == MOQ_OK);
    CHECK(moq_mpeg2ts_switcher_process(sw, pkt_b) == MOQ_OK);
    CHECK(rd_pmt_pcr(pkt_a) == 0x101 && rd_psi_version(pkt_a) == 1);
    CHECK(rd_pmt_pcr(pkt_b) == 0x102 && rd_psi_version(pkt_b) == 1);
    free(sw);
}

/* With program_number 0 the reference program is the first program of the
 * PAT (Unmodified Multiplex), not of whichever section came last. */
static void test_first_program_of_pat(void)
{
    for (int order = 0; order < 2; order++) {
        uint8_t sec_a[64], sec_b[64], psi[4 * 188], pkt[188];
        tb_prog_t progs_a[] = { { 1, 0x100 } };
        tb_prog_t progs_b[] = { { 2, 0x200 } };
        size_t na = tb_pat(sec_a, 1, 0, progs_a, 1);
        size_t nb = tb_pat(sec_b, 1, 0, progs_b, 1);
        tb_section_numbers(sec_a, na, 0, 1);
        tb_section_numbers(sec_b, nb, 1, 1);
        psi_pkt(psi + (order ? 188 : 0), 0, (uint8_t)(order ? 1 : 0), sec_a, na);
        psi_pkt(psi + (order ? 0 : 188), 0, (uint8_t)(order ? 0 : 1), sec_b, nb);
        tb_es_t es_a[] = { { 0x1b, 0x101, NULL, 0 } };
        tb_es_t es_b[] = { { 0x1b, 0x201, NULL, 0 } };
        na = tb_pmt(sec_a, 1, 0, 0x101, NULL, 0, es_a, 1);
        nb = tb_pmt(sec_b, 2, 0, 0x201, NULL, 0, es_b, 1);
        psi_pkt(psi + 376, 0x100, 0, sec_a, na);
        psi_pkt(psi + 564, 0x200, 0, sec_b, nb);
        static moq_mpeg2ts_receiver_t rx;
        moq_mpeg2ts_reorder_slot_t slots[4];
        moq_mpeg2ts_receive_result_t r;
        new_window_rx(&rx, slots);
        CHECK(moq_mpeg2ts_receiver_push(&rx, 0, 0, 0, B(psi, sizeof(psi)), &r, sizeof(r)) == MOQ_OK);
        video_pkt(pkt, true, 9000, 0);
        CHECK(moq_mpeg2ts_receiver_push(&rx, 0, 1, 0, B(pkt, sizeof(pkt)), &r, sizeof(r)) == MOQ_OK);
        CHECK(r.random_access);
        /* Repetitions of section 1 keep program 1. */
        uint8_t again[188];
        nb = tb_pat(sec_b, 1, 0, progs_b, 1);
        tb_section_numbers(sec_b, nb, 1, 1);
        psi_pkt(again, 0, 2, sec_b, nb);
        CHECK(moq_mpeg2ts_receiver_push(&rx, 0, 2, 0, B(again, 188), &r, sizeof(r)) == MOQ_OK);
        video_pkt(pkt, true, 12000, 1);
        CHECK(moq_mpeg2ts_receiver_push(&rx, 0, 3, 0, B(pkt, sizeof(pkt)), &r, sizeof(r)) == MOQ_OK);
        CHECK(r.random_access);
        /* A new version whose section 0 lists program 2 first moves to it:
         * video on 0x101 no longer counts. */
        tb_prog_t moved[] = { { 2, 0x200 } };
        na = tb_pat(sec_a, 1, 1, moved, 1);
        psi_pkt(again, 0, 3, sec_a, na);
        CHECK(moq_mpeg2ts_receiver_push(&rx, 0, 4, 0, B(again, 188), &r, sizeof(r)) == MOQ_OK);
        uint8_t pmt2[188];
        nb = tb_pmt(sec_b, 2, 0, 0x201, NULL, 0, es_b, 1);
        psi_pkt(pmt2, 0x200, 1, sec_b, nb);
        CHECK(moq_mpeg2ts_receiver_push(&rx, 0, 5, 0, B(pmt2, 188), &r, sizeof(r)) == MOQ_OK);
        video_pkt(pkt, true, 15000, 2);
        CHECK(moq_mpeg2ts_receiver_push(&rx, 0, 6, 0, B(pkt, sizeof(pkt)), &r, sizeof(r)) == MOQ_OK);
        CHECK(!r.random_access);
    }
}

/* Random access announcements of different PIDs do not overwrite each
 * other (2.4.3.5: each announces the next PES of its own PID). */
static void test_interleaved_announcements(void)
{
    uint8_t ann[3 * 188];
    tb_pkt_t va = { .pid = 0x101, .rai = true, .no_payload = true };
    tb_pkt_t aa = { .pid = 0x102, .rai = true, .no_payload = true };
    tb_packet(ann, &va);
    tb_packet(ann + 188, &aa);
    video_pkt(ann + 376, false, 9000, 0);
    static moq_mpeg2ts_receiver_t rx;
    moq_mpeg2ts_reorder_slot_t slots[4];
    moq_mpeg2ts_receive_result_t r;
    new_window_rx(&rx, slots);
    CHECK(moq_mpeg2ts_receiver_push(&rx, 0, 0, 0, B(ann, sizeof(ann)), &r, sizeof(r)) == MOQ_OK);
    CHECK(r.random_access);
    /* Adjacent control. */
    uint8_t adj[2 * 188];
    memcpy(adj, ann, 188);
    memcpy(adj + 188, ann + 376, 188);
    new_window_rx(&rx, slots);
    CHECK(moq_mpeg2ts_receiver_push(&rx, 0, 0, 0, B(adj, sizeof(adj)), &r, sizeof(r)) == MOQ_OK);
    CHECK(r.random_access);
    /* Inspection and the packetizer share the rule. */
    moq_mpeg2ts_inspect_cfg_t ip;
    moq_mpeg2ts_inspect_cfg_init(&ip);
    ip.random_access_pid = MOQ_MPEG2TS_PID_AUTO;
    moq_mpeg2ts_object_info_t info;
    CHECK(moq_mpeg2ts_inspect_object(&ip, B(ann, sizeof(ann)), &info, sizeof(info)) == MOQ_OK);
    CHECK(info.has_random_access && info.random_access_index == 2);
    static emitted_t e;
    memset(&e, 0, sizeof(e));
    uint8_t obj[188 * 4];
    moq_mpeg2ts_packetizer_cfg_t cfg;
    moq_mpeg2ts_packetizer_cfg_init(&cfg);
    cfg.object_packets = 4;
    cfg.object_buf = obj;
    cfg.object_cap = sizeof(obj);
    cfg.emit = on_emit;
    cfg.ctx = &e;
    moq_mpeg2ts_packetizer_t *pz = calloc(1, sizeof(*pz));
    CHECK(moq_mpeg2ts_packetizer_init(pz, sizeof(*pz), &cfg) == MOQ_OK);
    for (size_t i = 0; i < 3; i++)
        moq_mpeg2ts_packetizer_push(pz, B(ann + i * 188, 188));
    moq_mpeg2ts_packetizer_flush(pz);
    CHECK(e.count == 1 && e.rap[0] && memcmp(e.bytes, ann + 376, 188) == 0);
    free(pz);
}

/* A video PES start on `pid`, with a PCR, announced by its own
 * random_access_indicator. */
static void video_pkt_on(uint8_t *pk, uint16_t pid, uint64_t pcr, uint8_t cc)
{
    uint8_t pes[32];
    size_t pn = tb_pes(pes, 0xE0, true, pcr + 1000, false, 0);
    tb_pkt_t v = { .pid = pid, .pusi = true, .rai = true, .cc = cc,
                   .pcr = true, .pcr_base = pcr, .payload = pes,
                   .payload_len = pn };
    tb_packet(pk, &v);
}

/*
 * Two programs share PMT PID 0x100 (PAT {1, 2}). The reference program is
 * program 1, the first of the PAT, whichever PMT arrives last: its video
 * (0x101) opens a Group and program 2's (0x201) does not. A new PAT version
 * listing program 2 first on the same PID moves the reference to program 2,
 * and a later program 1 PMT on that PID no longer applies. Receiver and
 * packetizer share the rule.
 */
static void shared_pmt_stream(uint8_t *pk, bool pmt2_first)
{
    uint8_t sec[64];
    tb_prog_t both[] = { { 1, 0x100 }, { 2, 0x100 } };
    tb_es_t es1[] = { { 0x1B, 0x101, NULL, 0 } };
    tb_es_t es2[] = { { 0x1B, 0x201, NULL, 0 } };
    size_t n = tb_pat(sec, 1, 0, both, 2);
    psi_pkt(pk, 0, 0, sec, n);
    n = tb_pmt(sec, 1, 0, 0x101, NULL, 0, es1, 1);
    psi_pkt(pk + (pmt2_first ? 2 : 1) * 188, 0x100, pmt2_first ? 1 : 0, sec, n);
    n = tb_pmt(sec, 2, 0, 0x201, NULL, 0, es2, 1);
    psi_pkt(pk + (pmt2_first ? 1 : 2) * 188, 0x100, pmt2_first ? 0 : 1, sec, n);
    video_pkt_on(pk + 3 * 188, 0x201, 9000, 0);       /* not a RAP */
    video_pkt_on(pk + 4 * 188, 0x101, 9000, 0);       /* RAP */
    tb_prog_t moved[] = { { 2, 0x100 }, { 1, 0x100 } };
    n = tb_pat(sec, 1, 1, moved, 2);
    psi_pkt(pk + 5 * 188, 0, 1, sec, n);
    n = tb_pmt(sec, 2, 0, 0x201, NULL, 0, es2, 1);
    psi_pkt(pk + 6 * 188, 0x100, 2, sec, n);
    n = tb_pmt(sec, 1, 0, 0x101, NULL, 0, es1, 1);
    psi_pkt(pk + 7 * 188, 0x100, 3, sec, n);
    video_pkt_on(pk + 8 * 188, 0x101, 12000, 1);      /* not a RAP */
    video_pkt_on(pk + 9 * 188, 0x201, 12000, 1);      /* RAP */
}

static void test_shared_pmt_pid(void)
{
    static const bool expect[10] = { false, false, false, false, true,
                                     false, false, false, false, true };
    for (int order = 0; order < 2; order++) {
        uint8_t pk[10 * 188];
        shared_pmt_stream(pk, order != 0);
        static moq_mpeg2ts_receiver_t rx;
        moq_mpeg2ts_reorder_slot_t slots[4];
        moq_mpeg2ts_receive_result_t r;
        new_window_rx(&rx, slots);
        for (uint64_t i = 0; i < 10; i++) {
            CHECK(moq_mpeg2ts_receiver_push(&rx, 0, i, 0, B(pk + i * 188, 188),
                                            &r, sizeof(r)) == MOQ_OK);
            if (r.random_access != expect[i])
                fprintf(stderr, "  receiver order %d packet %u\n", order,
                        (unsigned)i);
            CHECK(r.random_access == expect[i]);
        }

        static emitted_t e;
        memset(&e, 0, sizeof(e));
        uint8_t obj[188];
        moq_mpeg2ts_packetizer_cfg_t cfg;
        moq_mpeg2ts_packetizer_cfg_init(&cfg);
        cfg.object_packets = 1;
        cfg.object_buf = obj;
        cfg.object_cap = sizeof(obj);
        cfg.emit = on_emit;
        cfg.ctx = &e;
        moq_mpeg2ts_packetizer_t *pz = calloc(1, sizeof(*pz));
        CHECK(moq_mpeg2ts_packetizer_init(pz, sizeof(*pz), &cfg) == MOQ_OK);
        for (size_t i = 0; i < 10; i++)
            CHECK(moq_mpeg2ts_packetizer_push(pz, B(pk + i * 188, 188)) == MOQ_OK);
        CHECK(moq_mpeg2ts_packetizer_flush(pz) == MOQ_OK);
        /* Groups open at packets 4 and 9; nothing before 4 is sent. */
        CHECK(e.count == 6);
        size_t groups = 0;
        for (size_t i = 0; i < e.count && i < 6; i++) {
            CHECK(memcmp(e.bytes + i * 188, pk + (4 + i) * 188, 188) == 0);
            CHECK(e.rap[i] == expect[4 + i]);
            CHECK((e.object[i] == 0) == expect[4 + i]);
            if (e.object[i] == 0) groups++;
        }
        CHECK(groups == 2);
        free(pz);
    }
}

/* Inspection selects the reference program as the receiver does: the first
 * program of the lowest-numbered section of the current PAT version, in
 * either arrival order; a later version moves it; an explicit program_number
 * still wins. */
static void inspect_pats(uint8_t *pk, bool reverse, bool new_version)
{
    uint8_t a[64], b[64];
    tb_prog_t pa[] = { { 1, 0x100 } }, pb[] = { { 2, 0x200 } };
    size_t na = tb_pat(a, 1, 0, pa, 1);
    size_t nb = tb_pat(b, 1, 0, pb, 1);
    tb_section_numbers(a, na, 0, 1);
    tb_section_numbers(b, nb, 1, 1);
    psi_pkt(pk + (reverse ? 188 : 0), 0, reverse ? 1 : 0, a, na);
    psi_pkt(pk + (reverse ? 0 : 188), 0, reverse ? 0 : 1, b, nb);
    tb_es_t ea[] = { { 0x1B, 0x101, NULL, 0 } }, eb[] = { { 0x1B, 0x201, NULL, 0 } };
    na = tb_pmt(a, 1, 0, 0x101, NULL, 0, ea, 1);
    nb = tb_pmt(b, 2, 0, 0x201, NULL, 0, eb, 1);
    psi_pkt(pk + 2 * 188, 0x100, 0, a, na);
    psi_pkt(pk + 3 * 188, 0x200, 0, b, nb);
    video_pkt_on(pk + 4 * 188, 0x101, 9000, 0);
    video_pkt_on(pk + 5 * 188, 0x201, 9000, 0);
    if (new_version) {                   /* version 1, program 2 alone */
        nb = tb_pat(b, 1, 1, pb, 1);
        psi_pkt(pk + 6 * 188, 0, 2, b, nb);
    } else {
        tb_pkt_t nul = { .pid = MOQ_MPEG2TS_PID_NULL };
        tb_packet(pk + 6 * 188, &nul);
    }
}

static void test_inspect_first_program(void)
{
    for (int mode = 0; mode < 5; mode++) {
        /* 0, 1: forward, reverse; 2: forward then a new version; 3, 4:
         * program 2 named, forward and reverse. */
        bool reverse = mode == 1 || mode == 4;
        bool named = mode >= 3;
        uint8_t pk[7 * 188];
        inspect_pats(pk, reverse, mode == 2);
        uint16_t want = (mode == 2 || named) ? 2 : 1;
        uint16_t pid = want == 1 ? 0x101 : 0x201;
        moq_mpeg2ts_inspect_cfg_t ip;
        moq_mpeg2ts_inspect_cfg_init(&ip);
        ip.random_access_pid = MOQ_MPEG2TS_PID_AUTO;
        if (named) ip.program_number = 2;
        moq_mpeg2ts_program_t progs[2];
        moq_mpeg2ts_pat_t pat;
        moq_mpeg2ts_pat_init(&pat, progs, 2);
        ip.pat = &pat;
        moq_mpeg2ts_object_info_t info;
        CHECK(moq_mpeg2ts_inspect_object(&ip, B(pk, sizeof(pk)), &info, sizeof(info)) == MOQ_OK);
        if (info.program_number != want)
            fprintf(stderr, "  inspection mode %d program %u\n", mode,
                    info.program_number);
        CHECK(info.has_pat && info.has_pmt && info.bad_crc_sections == 0);
        CHECK(info.program_number == want);
        CHECK(info.pmt_pid == (want == 1 ? 0x100 : 0x200));
        CHECK(info.pmt_pcr_pid == pid && info.pmt_video_pid == pid);
        CHECK(info.has_random_access);
        CHECK(info.random_access_index == (want == 1 ? 4u : 5u));
        CHECK(pat.program_count == 1 && progs[0].program_number == want);
    }
}

/*
 * The section-start flag is part of a packet's identity. A continuation
 * (PUSI 0) received with no section in progress is set aside; a packet on
 * the same PID with the same counter and the same payload bytes but PUSI 1
 * is a different packet, and the section it starts is read. An exact
 * repetition of that packet is still a duplicate.
 */
static void test_duplicate_includes_section_start(void)
{
    uint8_t sec[64], start[188], cont[188];
    tb_prog_t progs[] = { { 1, 0x100 } };
    size_t n = tb_pat(sec, 1, 0, progs, 1);
    psi_pkt(start, 0, 0, sec, n);
    memcpy(cont, start, 188);
    cont[1] &= (uint8_t)~0x40u;

    uint8_t storage[MOQ_MPEG2TS_SECTION_MAX];
    moq_mpeg2ts_section_buf_t sb;
    CHECK(moq_mpeg2ts_section_buf_init(&sb, sizeof(sb), storage, sizeof(storage)) ==
          MOQ_OK);
    static got_t g;
    memset(&g, 0, sizeof(g));
    feed(&sb, cont, &g);
    CHECK(g.n == 0);
    feed(&sb, start, &g);
    CHECK(g.n == 1 && g.len[0] == n && memcmp(g.sec[0], sec, n) == 0);
    feed(&sb, start, &g);                /* an exact duplicate */
    CHECK(g.n == 1);
    /* Control: the same pair with consecutive counters. */
    uint8_t start1[188];
    memcpy(start1, start, 188);
    start1[3] = (uint8_t)((start1[3] & 0xF0u) | 1u);
    moq_mpeg2ts_section_buf_reset(&sb);
    memset(&g, 0, sizeof(g));
    feed(&sb, cont, &g);
    feed(&sb, start1, &g);
    CHECK(g.n == 1);

    /* The switcher: after an output PAT of version 4, the PAT the packet
     * starts is rewritten to version 5 with a valid CRC_32, and its exact
     * duplicate is rewritten identically. */
    for (int repeated = 0; repeated < 2; repeated++) {
        uint8_t initial[188], c[188], s[188], dup[188];
        n = tb_pat(sec, 1, 4, progs, 1);
        psi_pkt(initial, 0, 7, sec, n);
        memcpy(c, cont, 188);
        memcpy(s, repeated ? start : start1, 188);
        memcpy(dup, s, 188);
        moq_mpeg2ts_switcher_t *sw = new_switcher();
        CHECK(moq_mpeg2ts_switcher_process(sw, initial) == MOQ_OK);
        CHECK(rd_psi_version(initial) == 4);
        moq_mpeg2ts_switcher_switch(sw, NULL, 0);
        CHECK(moq_mpeg2ts_switcher_process(sw, c) == MOQ_OK);
        CHECK(moq_mpeg2ts_switcher_process(sw, s) == MOQ_OK);
        if (rd_psi_version(s) != 5)
            fprintf(stderr, "  switcher repeated %d version %u\n", repeated,
                    rd_psi_version(s));
        CHECK(rd_psi_version(s) == 5);
        CHECK(moq_mpeg2ts_switcher_process(sw, dup) == MOQ_OK);
        CHECK(memcmp(dup, s, 188) == 0);
        free(sw);
    }
}

/*
 * Ending an Object mid-Group: the Object being filled is emitted inside the
 * open Group, and a PAT/PMT run held in the lead-in stays held, so that it
 * opens the next Group with its random access point. flush, by contrast,
 * ends the source: the run goes into the open Group.
 */
static void test_packetizer_end_object(void)
{
    uint8_t psi[2 * 188], rap0[188], rap1[188], c[3][188];
    prog_psi(psi);
    video_pkt(rap0, true, 9000, 0);
    video_pkt(rap1, true, 12000, 3);
    for (int k = 0; k < 3; k++) {
        tb_pkt_t t = { .pid = 0x101, .cc = (uint8_t)(k + 1) };
        tb_packet(c[k], &t);
    }
    static emitted_t e;
    memset(&e, 0, sizeof(e));
    uint8_t obj[188 * 8], lb[188 * 4];
    moq_mpeg2ts_packetizer_cfg_t cfg;
    moq_mpeg2ts_packetizer_cfg_init(&cfg);
    cfg.object_packets = 8;
    cfg.object_buf = obj;
    cfg.object_cap = sizeof(obj);
    cfg.lead_buf = lb;
    cfg.lead_cap = sizeof(lb);
    cfg.emit = on_emit;
    cfg.ctx = &e;
    moq_mpeg2ts_packetizer_t *pz = calloc(1, sizeof(*pz));
    CHECK(moq_mpeg2ts_packetizer_init(pz, sizeof(*pz), &cfg) == MOQ_OK);
#define PUSH(pk) CHECK(moq_mpeg2ts_packetizer_push(pz, B(pk, 188)) == MOQ_OK)
    PUSH(psi); PUSH(psi + 188); PUSH(rap0); PUSH(c[0]); PUSH(c[1]);
    CHECK(moq_mpeg2ts_packetizer_end_object(pz) == MOQ_OK);
    CHECK(e.count == 1 && e.packets[0] == 5);
    CHECK(moq_mpeg2ts_packetizer_end_object(pz) == MOQ_OK);   /* empty */
    CHECK(e.count == 1);
    PUSH(c[2]); PUSH(psi); PUSH(psi + 188);
    CHECK(moq_mpeg2ts_packetizer_end_object(pz) == MOQ_OK);
    PUSH(rap1);
#undef PUSH
    CHECK(moq_mpeg2ts_packetizer_flush(pz) == MOQ_OK);
    /* (0,0) PAT PMT RAP c c; (0,1) c; (1,0) PAT PMT RAP. */
    CHECK(e.count == 3);
    CHECK(e.group[1] == 0 && e.object[1] == 1 && e.packets[1] == 1);
    CHECK(memcmp(e.bytes + 5 * 188, c[2], 188) == 0);
    CHECK(e.group[2] == 1 && e.object[2] == 0 && e.rap[2] && e.packets[2] == 3);
    CHECK(memcmp(e.bytes + 6 * 188, psi, 2 * 188) == 0);
    CHECK(memcmp(e.bytes + 8 * 188, rap1, 188) == 0);
    moq_mpeg2ts_packetizer_stats_t st;
    CHECK(moq_mpeg2ts_packetizer_get_stats(pz, &st, sizeof(st)) == MOQ_OK);
    CHECK(st.groups == 2 && st.objects == 3);
    CHECK(moq_mpeg2ts_packetizer_end_object(NULL) == MOQ_ERR_INVAL);
    free(pz);
}

/* Polls answer MOQ_DONE when nothing is due and MOQ_ERR_INVAL for a bad
 * argument; stats copies are bounded by out_size and stamp what they wrote;
 * the library stamps its sized output records. */
static void test_result_and_size_contracts(void)
{
    moq_mpeg2ts_receiver_t rx;
    moq_mpeg2ts_reorder_slot_t slots[4];
    new_window_rx(&rx, slots);
    moq_mpeg2ts_delivery_t d;
    CHECK(moq_mpeg2ts_receiver_poll(&rx, 0, &d, sizeof(d)) == MOQ_DONE);
    CHECK(moq_mpeg2ts_receiver_poll(NULL, 0, &d, sizeof(d)) == MOQ_ERR_INVAL);
    CHECK(moq_mpeg2ts_receiver_poll(&rx, 0, NULL, 0) == MOQ_ERR_INVAL);
    CHECK(moq_mpeg2ts_receiver_next_deadline_us(&rx) == UINT64_MAX);

    uint8_t storage[MOQ_MPEG2TS_SECTION_MAX];
    moq_mpeg2ts_section_buf_t sb;
    moq_bytes_t sec;
    CHECK(moq_mpeg2ts_section_buf_init(&sb, sizeof(sb), storage, sizeof(storage)) ==
          MOQ_OK);
    CHECK(moq_mpeg2ts_section_buf_next(&sb, &sec) == MOQ_DONE);
    CHECK(moq_mpeg2ts_section_buf_next(NULL, &sec) == MOQ_ERR_INVAL);

    moq_mpeg2ts_switcher_t *sw = new_switcher();
    struct { moq_mpeg2ts_switcher_stats_t st; uint8_t canary[8]; } big;
    memset(&big, 0xCC, sizeof(big));
    CHECK(moq_mpeg2ts_switcher_get_stats(sw, &big.st, sizeof(big)) == MOQ_OK);
    CHECK(big.st.struct_size == sizeof(moq_mpeg2ts_switcher_stats_t));
    CHECK(big.st.switches == 0);
    for (size_t i = 0; i < sizeof(big.canary); i++) CHECK(big.canary[i] == 0xCC);
    CHECK(moq_mpeg2ts_switcher_get_stats(sw, &big.st, sizeof(uint32_t)) ==
          MOQ_ERR_INVAL);
    CHECK(moq_mpeg2ts_switcher_get_stats(NULL, &big.st, sizeof(big.st)) ==
          MOQ_ERR_INVAL);
    free(sw);

    moq_mpeg2ts_object_report_t rep;
    moq_mpeg2ts_object_report_init(&rep);
    CHECK(rep.struct_size == sizeof(rep) && rep.reason == MOQ_MPEG2TS_OK);
    moq_mpeg2ts_object_info_t info;
    moq_mpeg2ts_object_info_init(&info);
    CHECK(info.struct_size == sizeof(info));
    moq_mpeg2ts_receive_result_t r;
    moq_mpeg2ts_receive_result_init(&r);
    CHECK(r.struct_size == sizeof(r));
    CHECK(MOQ_MPEG2TS_OBJ_SUBGROUP_ENDS_GROUP == 1u &&
          MOQ_MPEG2TS_OBJ_LAST_IN_GROUP == 2u);
}

int main(void)
{
    test_section_buf_packed();
    test_pmt_validation();
    test_timing_validation();
    test_inspect_pmt_table_bounds();
    test_sized_inputs();
    test_filter_multisection_pat();
    test_filter_packed_pmt();
    test_filter_ca();
    test_filter_relocation();
    test_switcher_tables();
    test_switcher_atomic();
    test_align_join_duplicate();
    test_receiver_random_access_pid();
    test_receiver_rejected_run();
    test_receiver_group_end();
    test_receiver_reorder();
    test_receiver_join_grace();
    test_packetizer_lead_in();
    test_packetizer_rai_before_pes();
    test_switch_renditions();
    test_receiver_end_bit_needs_fin();
    test_receiver_end_group_at();
    test_receiver_deadline_progress();
    test_receiver_known_loss_across_groups();
    test_duplicates_are_byte_equal();
    test_first_program_of_pat();
    test_interleaved_announcements();
    test_shared_pmt_pid();
    test_inspect_first_program();
    test_duplicate_includes_section_start();
    test_packetizer_end_object();
    test_result_and_size_contracts();

    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("mpeg2ts state: all tests passed\n");
    return 0;
}
