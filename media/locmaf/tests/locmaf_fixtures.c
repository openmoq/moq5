#include "locmaf_fixtures.h"

int lm_failures = 0;

void lm_check_bytes(const char *file, int line, const char *what,
                    const uint8_t *a, size_t alen, const uint8_t *b, size_t blen)
{
    size_t n = alen < blen ? alen : blen;
    size_t i = 0;
    while (i < n && a[i] == b[i]) i++;
    if (i == n && alen == blen) return;
    fprintf(stderr, "FAIL: %s:%d: %s: lengths %zu vs %zu, first difference at %zu",
            file, line, what, alen, blen, i);
    if (i < n) fprintf(stderr, " (0x%02x vs 0x%02x)", a[i], b[i]);
    fprintf(stderr, "\n");
    lm_failures++;
}

int lm_finish(const char *name)
{
    printf("%s: %s: %d failures\n", lm_failures ? "FAIL" : "PASS", name, lm_failures);
    return lm_failures ? 1 : 0;
}

static int hexval(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

size_t lm_hex(const char *hex, uint8_t *out, size_t cap)
{
    size_t n = 0;
    int hi = -1;
    for (const char *p = hex; *p; p++) {
        if (*p == ' ' || *p == '\n' || *p == '\t') continue;
        int v = hexval(*p);
        if (v < 0) { fprintf(stderr, "lm_hex: bad digit '%c'\n", *p); exit(2); }
        if (hi < 0) { hi = v; continue; }
        if (n == cap) { fprintf(stderr, "lm_hex: overflow\n"); exit(2); }
        out[n++] = (uint8_t)((hi << 4) | v);
        hi = -1;
    }
    if (hi >= 0) { fprintf(stderr, "lm_hex: odd digit count\n"); exit(2); }
    return n;
}

void lm_fill(uint8_t *out, size_t len, uint8_t seed)
{
    for (size_t i = 0; i < len; i++) out[i] = (uint8_t)(seed + i);
}

/* -- Tracks ----------------------------------------------------------------- */

static moq_locmaf_track_t track_base(uint32_t dur, uint32_t flags)
{
    moq_locmaf_track_t t;
    moq_locmaf_track_init(&t);
    t.track_id = 1;
    t.trex_default_sample_description_index = 1;
    t.trex_default_sample_duration = dur;
    t.trex_default_sample_size = 0;
    t.trex_default_sample_flags = flags;
    return t;
}

moq_locmaf_track_t lm_track_a(void) { return track_base(1024, 0x02000000u); }
moq_locmaf_track_t lm_track_v(void) { return track_base(0, 0x01010000u); }

moq_locmaf_track_t lm_track_p8(void)
{
    moq_locmaf_track_t t = lm_track_a();
    t.is_protected = true;
    t.default_per_sample_iv_size = 8;
    return t;
}

moq_locmaf_track_t lm_track_p0(void)
{
    moq_locmaf_track_t t = lm_track_a();
    t.is_protected = true;
    t.default_per_sample_iv_size = 0;
    return t;
}

moq_locmaf_track_t lm_track_p16(void)
{
    moq_locmaf_track_t t = lm_track_p8();
    t.default_per_sample_iv_size = 16;
    return t;
}

moq_locmaf_track_t lm_fixture_track(const lm_fixture_t *f)
{
    switch (f->track) {
    case LM_TRACK_V:  return lm_track_v();
    case LM_TRACK_P8: return lm_track_p8();
    case LM_TRACK_P0: return lm_track_p0();
    default:          return lm_track_a();
    }
}

/* -- Golden fixtures (hand-built from sections 7 to 15) --------------------- */

#define F1_MOOF \
    "0000005C 6D6F6F66 00000010 6D666864 00000000 00000000 00000044 74726166" \
    "00000014 74666864 00020010 00000001 00000004"                          \
    "00000014 74666474 01000000 00000000 00000000"                          \
    "00000014 7472756E 00000001 00000001 00000064"                          \
    "0000000C 6D646174"

static const lm_fixture_t f1[] = {
    { "F1", LM_TRACK_A, 0, "02 04 0A 00 0E 01", F1_MOOF, 4, "DEADBEEF", 0 },
    { "F2", LM_TRACK_A, 1, "03 00",
      "0000005C 6D6F6F66 00000010 6D666864 00000000 00000000 00000044 74726166"
      "00000014 74666864 00020010 00000001 00000004"
      "00000014 74666474 01000000 00000000 00000400"
      "00000014 7472756E 00000001 00000001 00000064"
      "0000000C 6D646174", 4, "CAFEBABE", 0 },
};

static const lm_fixture_t f3[] = {
    { "F3A", LM_TRACK_V, 0, "02 0E 04 8B B8 08 E2 00 00 00 0A CD BB A0 0E 01",
      "00000064 6D6F6F66 00000010 6D666864 00000000 00000000 0000004C 74726166"
      "0000001C 74666864 00020038 00000001 00000BB8 00000005 02000000"
      "00000014 74666474 01000000 00000000 000DBBA0"
      "00000014 7472756E 00000001 00000001 0000006C"
      "0000000D 6D646174", 5, NULL, 0x30 },
    { "F3B", LM_TRACK_V, 1, "03 03 1B 01 08",
      "00000060 6D6F6F66 00000010 6D666864 00000000 00000000 00000048 74726166"
      "00000018 74666864 00020018 00000001 00000BB8 00000007"
      "00000014 74666474 01000000 00000000 000DC758"
      "00000014 7472756E 00000001 00000001 00000068"
      "0000000F 6D646174", 7, NULL, 0x40 },
    { "F3C", LM_TRACK_V, 2, "03 00",
      "00000060 6D6F6F66 00000010 6D666864 00000000 00000000 00000048 74726166"
      "00000018 74666864 00020018 00000001 00000BB8 00000007"
      "00000014 74666474 01000000 00000000 000DD310"
      "00000014 7472756E 00000001 00000001 00000068"
      "0000000F 6D646174", 7, NULL, 0x50 },
};

static const lm_fixture_t f4[] = {
    { "F4", LM_TRACK_A, 0, "02 09 01 02 06 05 0A 88 00 0E 03",
      "00000064 6D6F6F66 00000010 6D666864 00000000 00000000 0000004C 74726166"
      "00000010 74666864 00020000 00000001"
      "00000014 74666474 01000000 00000000 00000800"
      "00000020 7472756E 00000201 00000003 0000006C 00000006 00000005 00000004"
      "00000017 6D646174", 15, NULL, 0x60 },
    { "F4b", LM_TRACK_A, 1, "03 04 01 02 02 00",
      "00000064 6D6F6F66 00000010 6D666864 00000000 00000000 0000004C 74726166"
      "00000010 74666864 00020000 00000001"
      "00000014 74666474 01000000 00000000 00001400"
      "00000020 7472756E 00000201 00000003 0000006C 00000007 00000005 00000003"
      "00000017 6D646174", 15, NULL, 0x70 },
};

static const lm_fixture_t f5[] = {
    { "F5a", LM_TRACK_A, 0, "02 07 01 01 0A 0A 00 0E 02",
      "00000060 6D6F6F66 00000010 6D666864 00000000 00000000 00000048 74726166"
      "00000010 74666864 00020000 00000001"
      "00000014 74666474 01000000 00000000 00000000"
      "0000001C 7472756E 00000201 00000002 00000068 0000000A 00000014"
      "00000026 6D646174", 30, NULL, 0x80 },
    { "F5b", LM_TRACK_A, 1, "03 06 01 02 00 28 0E 02",
      "00000064 6D6F6F66 00000010 6D666864 00000000 00000000 0000004C 74726166"
      "00000010 74666864 00020000 00000001"
      "00000014 74666474 01000000 00000000 00000800"
      "00000020 7472756E 00000201 00000003 0000006C 0000000A 00000014 0000001E"
      "00000044 6D646174", 60, NULL, 0x90 },
    { "F5c", LM_TRACK_A, 2, "03 05 0E 03 1B 01 01",
      "0000005C 6D6F6F66 00000010 6D666864 00000000 00000000 00000044 74726166"
      "00000014 74666864 00020010 00000001 00000005"
      "00000014 74666474 01000000 00000000 00001400"
      "00000014 7472756E 00000001 00000001 00000064"
      "0000000D 6D646174", 5, NULL, 0xA0 },
};

static const lm_fixture_t f6[] = {
    { "F6", LM_TRACK_V, 0,
      "02 17 01 02 64 32 04 8B B8 05 05 97 70 97 6F 00 0A 00 0C E2 00 00 00 0E 03",
      "00000078 6D6F6F66 00000010 6D666864 00000000 00000000 00000060 74726166"
      "00000014 74666864 00020008 00000001 00000BB8"
      "00000014 74666474 01000000 00000000 00000000"
      "00000030 7472756E 01000A05 00000003 00000080 02000000"
      "00000064 00000BB8 00000032 FFFFF448 00000032 00000000"
      "000000D0 6D646174", 200, NULL, 0xB0 },
};

#define F7_STYP_UUID_OBJ \
    "01 14 73747970 636D6673 00000000 636D6673 636D6663" \
    "01 15 75756964 000102030405060708090A0B0C0D0E0F 78"
#define F7_STYP_UUID_BOX \
    "00000018 73747970 636D6673 00000000 636D6673 636D6663" \
    "00000019 75756964 000102030405060708090A0B0C0D0E0F 78"

static const lm_fixture_t f7[] = {
    { "F7", LM_TRACK_A, 0, F7_STYP_UUID_OBJ "02 04 0A 00 0E 01",
      F7_STYP_UUID_BOX F1_MOOF, 4, "DEADBEEF", 0 },
};

#define F8_EMSG \
    "01000000 00017700 0000000000017700 FFFFFFFF 00000001 75726E3A7400 00 6869"

static const lm_fixture_t f8[] = {
    { "F8", LM_TRACK_A, 0, "01 25 656D7367" F8_EMSG "02 06 0A C1 77 00 0E 00",
      "00000029 656D7367" F8_EMSG
      "00000058 6D6F6F66 00000010 6D666864 00000000 00000000 00000040 74726166"
      "00000010 74666864 00020000 00000001"
      "00000014 74666474 01000000 00000000 00017700"
      "00000014 7472756E 00000001 00000000 00000060"
      "00000008 6D646174", 0, "", 0 },
};

static const lm_fixture_t f9[] = {
    { "F9", LM_TRACK_P8, 0,
      "02 27 01 01 64 09 10 0102030405060708090A0B0C0D0E0F10 0A 00"
      "0B 02 01 02 0D 03 0A 05 05 0E 02 0F 03 5A 14 1E",
      "000000BD 6D6F6F66 00000010 6D666864 00000000 00000000 000000A5 74726166"
      "00000010 74666864 00020000 00000001"
      "00000014 74666474 01000000 00000000 00000000"
      "0000001C 7472756E 00000201 00000002 000000C5 00000064 0000003C"
      "00000013 7361697A 00000000 00 00000002 10 16"
      "00000014 7361696F 00000000 00000001 00000097"
      "00000036 73656E63 00000002 00000002"
      "0102030405060708 0001 000A 0000005A"
      "090A0B0C0D0E0F10 0002 0005 00000014 0005 0000001E"
      "000000A8 6D646174", 160, NULL, 0xC0 },
};

static const lm_fixture_t f10[] = {
    { "F10", LM_TRACK_P0, 0, "02 0D 0A 00 0B 01 01 0D 01 10 0E 01 0F 01 54",
      "00000099 6D6F6F66 00000010 6D666864 00000000 00000000 00000081 74726166"
      "00000014 74666864 00020010 00000001 00000064"
      "00000014 74666474 01000000 00000000 00000000"
      "00000014 7472756E 00000001 00000001 000000A1"
      "00000011 7361697A 00000000 08 00000001"
      "00000014 7361696F 00000000 00000001 00000091"
      "00000018 73656E63 00000002 00000001 0001 0010 00000054"
      "0000006C 6D646174", 100, NULL, 0xD0 },
};

static const lm_fixture_t f11[] = {
    { "F11", LM_TRACK_P0, 0, "02 04 0A 00 0E 01", F1_MOOF, 4, "DEADBEEF", 0 },
};

const lm_sequence_t lm_sequences[] = {
    { "F1-F2", f1, 2 }, { "F3", f3, 3 }, { "F4", f4, 2 }, { "F5", f5, 3 },
    { "F6", f6, 1 }, { "F7", f7, 1 }, { "F8", f8, 1 }, { "F9", f9, 1 },
    { "F10", f10, 1 }, { "F11", f11, 1 },
};
const size_t lm_sequence_count = sizeof(lm_sequences) / sizeof(lm_sequences[0]);

static size_t fixture_payload(const lm_fixture_t *f, uint8_t *out, size_t cap)
{
    if (f->payload_len > cap) { fprintf(stderr, "fixture payload overflow\n"); exit(2); }
    if (f->payload_hex) return lm_hex(f->payload_hex, out, cap);
    lm_fill(out, f->payload_len, f->payload_seed);
    return f->payload_len;
}

size_t lm_fixture_object(const lm_fixture_t *f, uint8_t *out, size_t cap)
{
    size_t n = lm_hex(f->object_hex, out, cap);
    return n + fixture_payload(f, out + n, cap - n);
}

size_t lm_fixture_canonical(const lm_fixture_t *f, uint8_t *out, size_t cap)
{
    size_t n = lm_hex(f->canonical_hex, out, cap);
    return n + fixture_payload(f, out + n, cap - n);
}

/* -- State setup -------------------------------------------------------------- */

moq_result_t lm_dec_open(lm_dec_t *dec, const moq_locmaf_track_t *track,
                         uint32_t max_samples, uint32_t max_subsamples,
                         uint32_t max_gen_boxes)
{
    memset(dec, 0, sizeof(*dec));
    moq_locmaf_decoder_cfg_t cfg;
    moq_locmaf_decoder_cfg_init(&cfg);
    cfg.max_samples = max_samples;
    cfg.max_subsamples = max_subsamples;
    cfg.max_gen_boxes = max_gen_boxes;
    cfg.track = track;
    moq_result_t rc = moq_locmaf_decoder_storage_size(&cfg, &dec->storage_size);
    if (rc != MOQ_OK) return rc;
    dec->storage = malloc(dec->storage_size);
    if (!dec->storage) return MOQ_ERR_NOMEM;
    cfg.storage = dec->storage;
    cfg.storage_size = dec->storage_size;
    rc = moq_locmaf_decoder_init(&dec->d, sizeof(dec->d), &cfg);
    if (rc != MOQ_OK) { free(dec->storage); dec->storage = NULL; }
    return rc;
}

void lm_dec_close(lm_dec_t *dec)
{
    free(dec->storage);
    dec->storage = NULL;
}

moq_result_t lm_enc_open(lm_enc_t *enc, const moq_locmaf_track_t *track,
                         uint32_t max_samples, uint32_t max_subsamples,
                         uint32_t max_gen_boxes)
{
    memset(enc, 0, sizeof(*enc));
    moq_locmaf_encoder_cfg_t cfg;
    moq_locmaf_encoder_cfg_init(&cfg);
    cfg.max_samples = max_samples;
    cfg.max_subsamples = max_subsamples;
    cfg.max_gen_boxes = max_gen_boxes;
    cfg.track = track;
    moq_result_t rc = moq_locmaf_encoder_storage_size(&cfg, &enc->storage_size);
    if (rc != MOQ_OK) return rc;
    enc->storage = malloc(enc->storage_size);
    if (!enc->storage) return MOQ_ERR_NOMEM;
    cfg.storage = enc->storage;
    cfg.storage_size = enc->storage_size;
    rc = moq_locmaf_encoder_init(&enc->e, sizeof(enc->e), &cfg);
    if (rc != MOQ_OK) { free(enc->storage); enc->storage = NULL; }
    return rc;
}

void lm_enc_close(lm_enc_t *enc)
{
    free(enc->storage);
    enc->storage = NULL;
}

moq_result_t lm_decode_write(lm_dec_t *dec, const moq_locmaf_track_t *track,
                             uint64_t g, uint64_t o, const uint8_t *obj,
                             size_t obj_len, uint8_t *out, size_t cap,
                             size_t *out_len, moq_locmaf_reason_t *why)
{
    moq_locmaf_chunk_t chunk;
    moq_result_t rc = moq_locmaf_decoder_push(&dec->d, g, o,
        (moq_bytes_t){ obj, obj_len }, &chunk, sizeof(chunk), why);
    if (rc != MOQ_OK) return rc;
    size_t need = 0;
    moq_result_t wrc = moq_locmaf_write_cmaf(track, &chunk, NULL, 0, &need);
    if (wrc != MOQ_ERR_BUFFER && wrc != MOQ_OK) return wrc;
    if (need > cap) return MOQ_ERR_BUFFER;
    wrc = moq_locmaf_write_cmaf(track, &chunk, out, cap, out_len);
    if (wrc == MOQ_OK && *out_len != need) return MOQ_ERR_INTERNAL;
    return wrc;
}

/* -- CMAF chunk builder ------------------------------------------------------- */

typedef struct wbuf { uint8_t *p; size_t n, cap; } wbuf_t;

static void wb_need(wbuf_t *w, size_t k)
{
    if (k > w->cap - w->n) { fprintf(stderr, "lm_build_chunk: overflow\n"); exit(2); }
}
static void wb_u8(wbuf_t *w, uint8_t v) { wb_need(w, 1); w->p[w->n++] = v; }
static void wb_u16(wbuf_t *w, uint16_t v) { wb_u8(w, (uint8_t)(v >> 8)); wb_u8(w, (uint8_t)v); }
static void wb_u32(wbuf_t *w, uint32_t v) { wb_u16(w, (uint16_t)(v >> 16)); wb_u16(w, (uint16_t)v); }
static void wb_u64(wbuf_t *w, uint64_t v) { wb_u32(w, (uint32_t)(v >> 32)); wb_u32(w, (uint32_t)v); }
static void wb_raw(wbuf_t *w, const void *src, size_t len)
{
    if (!len) return;
    wb_need(w, len);
    memcpy(w->p + w->n, src, len);
    w->n += len;
}
static void wb_box(wbuf_t *w, uint32_t size, const char *type)
{
    wb_u32(w, size);
    wb_raw(w, type, 4);
}

static size_t per_sample_fields(uint32_t fl)
{
    return (size_t)((fl & 0x100) != 0) + ((fl & 0x200) != 0) +
           ((fl & 0x400) != 0) + ((fl & 0x800) != 0);
}

size_t lm_build_chunk(const lm_chunk_spec_t *s, uint8_t *out, size_t cap)
{
    wbuf_t w = { out, 0, cap };
    for (size_t k = 0; k < s->gen_box_count; k++) {
        char t[4] = { (char)(s->gen_boxes[k].type >> 24), (char)(s->gen_boxes[k].type >> 16),
                      (char)(s->gen_boxes[k].type >> 8), (char)s->gen_boxes[k].type };
        wb_box(&w, (uint32_t)(8 + s->gen_boxes[k].len), t);
        wb_raw(&w, s->gen_boxes[k].payload, s->gen_boxes[k].len);
    }

    size_t tfhd = 16 + ((s->tfhd_flags & 0x01) ? 8u : 0u) +
                  4u * (size_t)(((s->tfhd_flags & 0x02) != 0) + ((s->tfhd_flags & 0x08) != 0) +
                                ((s->tfhd_flags & 0x10) != 0) + ((s->tfhd_flags & 0x20) != 0));
    size_t tfdt = s->tfdt_version == 1 ? 20 : 16;
    size_t trun = 16 + ((s->trun_flags & 0x001) ? 4u : 0u) + ((s->trun_flags & 0x004) ? 4u : 0u) +
                  (size_t)s->n * 4 * per_sample_fields(s->trun_flags);
    size_t aux_sum = 0;
    uint32_t subs_total = 0;
    if (s->senc) {
        for (uint32_t i = 0; i < s->n; i++) {
            size_t c = s->sub_counts ? s->sub_counts[i] : 0;
            aux_sum += s->iv + (s->sub_counts ? 2 + 6 * c : 0);
            subs_total += (uint32_t)c;
        }
    }
    bool aux_same = true;
    uint8_t aux0 = 0;
    for (uint32_t i = 0; s->senc && i < s->n; i++) {
        size_t a = s->iv + (s->sub_counts ? 2 + 6 * (size_t)s->sub_counts[i] : 0);
        if (i == 0) aux0 = (uint8_t)a;
        else if (a != aux0) aux_same = false;
    }
    size_t saiz = (s->senc && s->saiz_saio) ? 17 + (aux_same ? 0 : s->n) : 0;
    size_t saio = (s->senc && s->saiz_saio) ? 20 : 0;
    size_t senc = s->senc ? 16 + aux_sum : 0;
    size_t traf = 8 + tfhd + tfdt + trun + saiz + saio + senc + s->traf_extra_len;
    size_t moof = 8 + 16 + traf + s->moof_extra_len;
    size_t senc_off = 8 + 16 + 8 + tfhd + tfdt + trun + saiz + saio;   /* in moof */

    wb_box(&w, (uint32_t)moof, "moof");
    wb_box(&w, 16, "mfhd");
    wb_u32(&w, 0);
    wb_u32(&w, s->mfhd_seq);
    wb_box(&w, (uint32_t)traf, "traf");
    wb_box(&w, (uint32_t)tfhd, "tfhd");
    wb_u32(&w, s->tfhd_flags);
    wb_u32(&w, s->track_id);
    if (s->tfhd_flags & 0x01) wb_u64(&w, 0);
    if (s->tfhd_flags & 0x02) wb_u32(&w, s->tfhd_sdi);
    if (s->tfhd_flags & 0x08) wb_u32(&w, s->tfhd_dur);
    if (s->tfhd_flags & 0x10) wb_u32(&w, s->tfhd_size);
    if (s->tfhd_flags & 0x20) wb_u32(&w, s->tfhd_flags_value);
    wb_box(&w, (uint32_t)tfdt, "tfdt");
    if (s->tfdt_version == 1) { wb_u32(&w, 0x01000000); wb_u64(&w, s->bmdt); }
    else { wb_u32(&w, 0); wb_u32(&w, (uint32_t)s->bmdt); }
    wb_box(&w, (uint32_t)trun, "trun");
    wb_u32(&w, ((uint32_t)s->trun_version << 24) | s->trun_flags);
    wb_u32(&w, s->n);
    if (s->trun_flags & 0x001)
        wb_u32(&w, s->data_offset_override ? s->data_offset : (uint32_t)(moof + 8));
    if (s->trun_flags & 0x004) wb_u32(&w, s->first_flags);
    for (uint32_t i = 0; i < s->n; i++) {
        if (s->trun_flags & 0x100) wb_u32(&w, s->durs[i]);
        if (s->trun_flags & 0x200) wb_u32(&w, s->sizes[i]);
        if (s->trun_flags & 0x400) wb_u32(&w, s->flags[i]);
        if (s->trun_flags & 0x800) wb_u32(&w, (uint32_t)s->ctos[i]);
    }
    if (saiz) {
        wb_box(&w, (uint32_t)saiz, "saiz");
        wb_u32(&w, 0);
        wb_u8(&w, aux_same ? aux0 : 0);
        wb_u32(&w, s->n);
        for (uint32_t i = 0; !aux_same && i < s->n; i++)
            wb_u8(&w, (uint8_t)(s->iv + (s->sub_counts ? 2 + 6 * s->sub_counts[i] : 0)));
        wb_box(&w, 20, "saio");
        wb_u32(&w, 0);
        wb_u32(&w, 1);
        wb_u32(&w, (uint32_t)(senc_off + 16));
    }
    if (senc) {
        wb_box(&w, (uint32_t)senc, "senc");
        wb_u32(&w, s->sub_counts ? 2u : 0u);
        wb_u32(&w, s->n);
        size_t j = 0;
        for (uint32_t i = 0; i < s->n; i++) {
            wb_raw(&w, s->ivs + (size_t)i * s->iv, s->iv);
            if (!s->sub_counts) continue;
            wb_u16(&w, s->sub_counts[i]);
            for (uint16_t k = 0; k < s->sub_counts[i]; k++, j++) {
                wb_u16(&w, s->clear[j]);
                wb_u32(&w, s->prot[j]);
            }
        }
        (void)subs_total;
    }
    wb_raw(&w, s->traf_extra, s->traf_extra_len);
    wb_raw(&w, s->moof_extra, s->moof_extra_len);
    wb_box(&w, (uint32_t)(8 + s->payload_len), "mdat");
    wb_raw(&w, s->payload, s->payload_len);
    return w.n;
}

/* -- Generator ---------------------------------------------------------------- */

uint64_t lm_rng_next(lm_rng_t *r)
{
    /* splitmix64 */
    uint64_t z = (r->s += 0x9E3779B97F4A7C15ull);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
}

uint32_t lm_rng_below(lm_rng_t *r, uint32_t n)
{
    return n ? (uint32_t)(lm_rng_next(r) % n) : 0;
}

static const uint8_t gen_emsg[] = {
    0x01, 0, 0, 0, 0, 0, 0x03, 0xE8, 0, 0, 0, 0, 0, 0, 0, 0x10,
    0, 0, 0, 0x20, 0, 0, 0, 0x07, 'u', 0, 'v', 0, 'm'
};
static const uint8_t gen_prft[] = {
    0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0x10, 0
};

size_t lm_gen_source(lm_rng_t *rng, const moq_locmaf_track_t *track,
                     uint64_t next_bmdt, bool first, lm_gen_chunk_t *g,
                     uint8_t *out, size_t cap)
{
    memset(g, 0, sizeof(*g));
    g->n = lm_rng_below(rng, 9);
    g->bmdt = (first || lm_rng_below(rng, 8) == 0) ? lm_rng_next(rng) % 5000000u
                                                   : next_bmdt;
    const uint32_t trex_dur = track->trex_default_sample_duration;
    const uint32_t trex_flags = track->trex_default_sample_flags;

    /* durations: trex, uniform other, or varying */
    uint32_t dmode = lm_rng_below(rng, 3);
    uint32_t ud = dmode == 0 ? trex_dur : 1000 + lm_rng_below(rng, 3000);
    /* sizes: uniform or varying; the payload follows */
    bool usize = lm_rng_below(rng, 2) == 0;
    uint32_t us = 1 + lm_rng_below(rng, 60);
    /* flags: all trex, all other, first differs, varying */
    uint32_t fmode = lm_rng_below(rng, 4);
    /* cto: none, non-negative, mixed */
    uint32_t cmode = lm_rng_below(rng, 3);
    for (uint32_t i = 0; i < g->n; i++) {
        g->durs[i] = dmode == 2 ? 500 + lm_rng_below(rng, 2000) : ud;
        g->sizes[i] = usize ? us : lm_rng_below(rng, 64);
        g->flags[i] = fmode == 0 ? trex_flags
                    : fmode == 1 ? 0x02000000u
                    : fmode == 2 ? (i == 0 ? 0x02000000u : trex_flags)
                    : (lm_rng_below(rng, 2) ? 0x02000000u : 0x01010000u);
        g->ctos[i] = cmode == 0 ? 0
                   : cmode == 1 ? (int64_t)lm_rng_below(rng, 6000)
                   : (int64_t)lm_rng_below(rng, 12000) - 6000;
        g->payload_len += g->sizes[i];
    }
    lm_fill(g->payload, g->payload_len, (uint8_t)lm_rng_next(rng));

    /* CENC on a protected track: none, IVs only, or IVs and subsamples */
    uint32_t emode = track->is_protected ? lm_rng_below(rng, 3) : 0;
    uint8_t iv = track->default_per_sample_iv_size;
    if (emode == 1 && iv == 0) emode = 2;
    /* a nonzero IV size means every sample carries its IV */
    if (emode == 0 && iv > 0) emode = 1;
    size_t j = 0;
    if (emode && g->n) {
        for (size_t b = 0; b < (size_t)g->n * iv; b++) g->ivs[b] = (uint8_t)lm_rng_next(rng);
        if (emode == 2) {
            for (uint32_t i = 0; i < g->n; i++) {
                uint32_t size = g->sizes[i];
                uint16_t c = size == 0 ? (uint16_t)lm_rng_below(rng, 2)
                                       : (uint16_t)(1 + lm_rng_below(rng, 3));
                if (c > 4) c = 4;
                g->sub_counts[i] = c;
                uint32_t left = size;
                for (uint16_t k = 0; k < c; k++, j++) {
                    uint32_t cl = k + 1 == c ? (left > 0 ? lm_rng_below(rng, left + 1) : 0)
                                             : lm_rng_below(rng, left / 2 + 1);
                    if (cl > 65535) cl = 65535;
                    uint32_t pr = k + 1 == c ? left - cl : lm_rng_below(rng, left - cl + 1);
                    g->clear[j] = (uint16_t)cl;
                    g->prot[j] = pr;
                    left -= cl + pr;
                }
            }
        }
    }

    /* genBoxes: none, an emsg, or a prft and an emsg */
    uint32_t gmode = lm_rng_below(rng, 3);
    if (gmode >= 1) {
        g->gen_boxes[g->gen_box_count++] = (lm_gen_box_t){
            LM_FOURCC_T('e','m','s','g'), gen_emsg, sizeof(gen_emsg) };
    }
    if (gmode == 2) {
        g->gen_boxes[g->gen_box_count++] = (lm_gen_box_t){
            LM_FOURCC_T('p','r','f','t'), gen_prft, sizeof(gen_prft) };
    }

    /* The muxer's layout choices. */
    lm_chunk_spec_t s;
    memset(&s, 0, sizeof(s));
    s.track_id = track->track_id;
    s.mfhd_seq = lm_rng_below(rng, 100);
    s.tfdt_version = g->bmdt <= 0xFFFFFFFFu && lm_rng_below(rng, 2) ? 0 : 1;
    s.bmdt = g->bmdt;
    s.tfhd_flags = lm_rng_below(rng, 2) ? 0x020000u : 0;
    s.trun_flags = 0x001;
    s.n = g->n;
    s.durs = g->durs;
    s.sizes = g->sizes;
    s.flags = g->flags;
    s.ctos = g->ctos;
    if (lm_rng_below(rng, 3) == 0) {
        s.tfhd_flags |= 0x02;
        s.tfhd_sdi = track->trex_default_sample_description_index;
    }
    bool dur_uniform = dmode != 2;
    if (dur_uniform && lm_rng_below(rng, 2)) {
        s.tfhd_flags |= 0x08;
        s.tfhd_dur = ud;
    } else if (!dur_uniform || ud != trex_dur || lm_rng_below(rng, 2)) {
        s.trun_flags |= 0x100;
    }
    if (usize && g->n > 0 && lm_rng_below(rng, 2)) {
        s.tfhd_flags |= 0x10;
        s.tfhd_size = us;
    } else {
        s.trun_flags |= 0x200;
    }
    if (fmode == 0 && lm_rng_below(rng, 2)) {
        /* rely on trex */
    } else if ((fmode == 0 || fmode == 1) && lm_rng_below(rng, 2)) {
        s.tfhd_flags |= 0x20;
        s.tfhd_flags_value = g->n ? g->flags[0] : trex_flags;
    } else if (fmode == 2 && g->n > 1 && lm_rng_below(rng, 2)) {
        s.trun_flags |= 0x004;
        s.first_flags = g->flags[0];
        s.tfhd_flags |= 0x20;
        s.tfhd_flags_value = g->flags[1];
    } else {
        s.trun_flags |= 0x400;
    }
    bool negative = false;
    for (uint32_t i = 0; i < g->n; i++) negative |= g->ctos[i] < 0;
    if (cmode != 0 || lm_rng_below(rng, 4) == 0) s.trun_flags |= 0x800;
    s.trun_version = (negative || ((s.trun_flags & 0x800) && lm_rng_below(rng, 2))) ? 1 : 0;
    if (emode && g->n) {
        s.senc = true;
        s.iv = iv;
        s.ivs = g->ivs;
        s.sub_counts = emode == 2 ? g->sub_counts : NULL;
        s.clear = g->clear;
        s.prot = g->prot;
        s.saiz_saio = lm_rng_below(rng, 2) == 0;
    }
    s.gen_box_count = g->gen_box_count;
    s.gen_boxes = g->gen_boxes;
    s.payload = g->payload;
    s.payload_len = g->payload_len;
    return lm_build_chunk(&s, out, cap);
}
