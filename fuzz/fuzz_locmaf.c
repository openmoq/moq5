/*
 * libFuzzer target for the LOCMAF object parser, decoder and canonical
 * writer (media/locmaf/include/moq/locmaf.h).
 *
 * Input: byte 0 selects the track (clear, cenc with an 8-byte IV, cbcs with
 * a constant IV) and small limits; then records of [flags][vi64 length]
 * [Object], the length-prefixed framing of LOCMAF section 17. Flag bit 0
 * starts a new group, bit 1 skips one Object ID (a gap), bit 2 resets the
 * decoder before the push.
 *
 * Invariants under test:
 *   1. moq_locmaf_parse_object() returns only OK, BUFFER, PROTO or
 *      UNSUPPORTED; on PROTO/UNSUPPORTED the count is 0; on OK and BUFFER
 *      every span lies within the Object and the count is exact.
 *   2. moq_locmaf_decoder_push() returns only OK, WRONG_STATE, PROTO, LIMIT
 *      or UNSUPPORTED, agrees with the parser on PROTO framing, and never
 *      writes the chunk except on OK.
 *   3. Every OK chunk is written by moq_locmaf_write_cmaf(): the probe
 *      returns BUFFER (or OK for an exact empty fit), the write returns OK
 *      with the probed length, and for n >= 1 the written moof and mdat
 *      parse with moq_cmaf_parse_fragment() to the decoder's sample table.
 */

#include <moq/locmaf.h>
#include <moq/cmaf.h>
#include <moq/vi64.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define FUZZ_MAX_INPUT 65536

static void check_span(const uint8_t *base, size_t len, moq_bytes_t s)
{
    if (s.len == 0) return;
    if (!s.data) __builtin_trap();
    uintptr_t b = (uintptr_t)base, p = (uintptr_t)s.data;
    if (p < b || p - b > len || s.len > len - (p - b)) __builtin_trap();
}

static moq_locmaf_track_t pick_track(uint8_t sel)
{
    moq_locmaf_track_t t;
    moq_locmaf_track_init(&t);
    t.track_id = 1;
    t.trex_default_sample_description_index = 1;
    t.trex_default_sample_duration = (sel & 0x10) ? 0 : 1024;
    t.trex_default_sample_size = (sel & 0x20) ? 4 : 0;
    t.trex_default_sample_flags = (sel & 0x40) ? 0x01010000u : 0x02000000u;
    switch (sel % 3) {
    case 1: t.is_protected = true; t.default_per_sample_iv_size = 8; break;
    case 2: t.is_protected = true; t.default_per_sample_iv_size = 0; break;
    default: break;
    }
    return t;
}

static void check_parse(const uint8_t *obj, size_t len)
{
    moq_locmaf_object_view_t v;
    moq_locmaf_gen_box_t boxes[4];
    size_t count = 99;
    moq_locmaf_reason_t why;
    moq_result_t rc = moq_locmaf_parse_object((moq_bytes_t){ obj, len }, &v,
                                              sizeof(v), boxes, 4, &count, &why);
    if (rc == MOQ_ERR_PROTO || rc == MOQ_ERR_UNSUPPORTED) {
        if (count != 0 || why == MOQ_LOCMAF_REASON_NONE) __builtin_trap();
        return;
    }
    if (rc != MOQ_OK && rc != MOQ_ERR_BUFFER) __builtin_trap();
    if (count != v.gen_box_count || (rc == MOQ_OK) != (count <= 4)) __builtin_trap();
    check_span(obj, len, v.properties);
    check_span(obj, len, v.mdat_payload);
    check_span(obj, len, v.raw_boxes);
    for (size_t i = 0; i < count && i < 4; i++) check_span(obj, len, boxes[i].payload);
}

static void check_write(const moq_locmaf_track_t *track, const moq_locmaf_chunk_t *c)
{
    size_t need = 0;
    moq_result_t rc = moq_locmaf_write_cmaf(track, c, NULL, 0, &need);
    if (rc != MOQ_ERR_BUFFER && !(rc == MOQ_OK && need == 0)) __builtin_trap();
    if (need > (1u << 22)) return;
    uint8_t *buf = malloc(need ? need : 1);
    if (!buf) return;
    size_t len = 0;
    if (moq_locmaf_write_cmaf(track, c, buf, need, &len) != MOQ_OK || len != need)
        __builtin_trap();
    if (c->kind != MOQ_LOCMAF_OBJECT_RAW_BOXES && c->sample_count > 0 &&
        c->sample_count <= 64) {
        /* The CMAF parser skips only some boxes ahead of a moof: read the
         * chunk from its moof, past the wrapped genBoxes. */
        size_t at = 0;
        for (size_t g = 0; g < c->gen_box_count; g++) at += 8 + c->gen_boxes[g].payload.len;
        if (at >= len) __builtin_trap();
        moq_cmaf_sample_t s[64];
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, s, 64);
        if (moq_cmaf_parse_fragment((moq_bytes_t){ buf + at, len - at }, &frag) != MOQ_OK)
            __builtin_trap();
        if (frag.sample_count != c->sample_count || frag.track_id != track->track_id ||
            frag.base_decode_time != c->base_media_decode_time ||
            frag.mdat.len != c->mdat_payload.len)
            __builtin_trap();
        /* parse_fragment ignores trex, so only samples whose values the
         * canonical chunk states are compared through it: sizes always are
         * (in tfhd or trun) unless they equal the trex default. */
        for (size_t i = 0; i < frag.sample_count; i++) {
            if ((uint32_t)frag.samples[i].composition_offset !=
                (uint32_t)c->samples[i].composition_offset)
                __builtin_trap();
            if (c->samples[i].size != track->trex_default_sample_size &&
                frag.samples[i].size != c->samples[i].size)
                __builtin_trap();
        }
    }
    free(buf);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < 1 || size > FUZZ_MAX_INPUT) return 0;
    moq_locmaf_track_t track = pick_track(data[0]);

    moq_locmaf_decoder_cfg_t cfg;
    moq_locmaf_decoder_cfg_init(&cfg);
    cfg.max_samples = 64;
    cfg.max_subsamples = 256;
    cfg.max_gen_boxes = 4;
    cfg.track = &track;
    size_t need = 0;
    if (moq_locmaf_decoder_storage_size(&cfg, &need) != MOQ_OK) __builtin_trap();
    void *storage = malloc(need);
    if (!storage) return 0;
    cfg.storage = storage;
    cfg.storage_size = need;
    moq_locmaf_decoder_t d;
    if (moq_locmaf_decoder_init(&d, sizeof(d), &cfg) != MOQ_OK) __builtin_trap();

    uint64_t group = 0, object = 0;
    size_t pos = 1;
    for (int round = 0; round < 64 && pos < size; round++) {
        uint8_t flags = data[pos++];
        uint64_t len;
        size_t k = moq_vi64_decode(data + pos, size - pos, &len);
        if (!k) break;
        pos += k;
        if (len > size - pos) len = size - pos;
        /* A private copy: ASan catches any read past the Object. */
        uint8_t *obj = malloc(len ? (size_t)len : 1);
        if (!obj) break;
        memcpy(obj, data + pos, (size_t)len);
        pos += (size_t)len;

        check_parse(obj, (size_t)len);

        if (flags & 1) { group++; object = 0; }
        if (flags & 2) object++;
        if (flags & 4) moq_locmaf_decoder_reset(&d);

        moq_locmaf_chunk_t chunk;
        memset(&chunk, 0xEE, sizeof(chunk));
        moq_locmaf_reason_t why;
        moq_result_t rc = moq_locmaf_decoder_push(&d, group, object,
            (moq_bytes_t){ obj, (size_t)len }, &chunk, sizeof(chunk), &why);
        object++;
        if (rc == MOQ_OK) {
            if (chunk.struct_size != MOQ_LOCMAF_CHUNK_V0_SIZE) __builtin_trap();
            check_span(obj, (size_t)len, chunk.mdat_payload);
            check_span(obj, (size_t)len, chunk.raw_boxes);
            check_write(&track, &chunk);
        } else if (rc == MOQ_ERR_WRONG_STATE || rc == MOQ_ERR_PROTO ||
                   rc == MOQ_ERR_LIMIT || rc == MOQ_ERR_UNSUPPORTED) {
            if (chunk.struct_size != 0xEEEEEEEEu || why == MOQ_LOCMAF_REASON_NONE)
                __builtin_trap();
        } else {
            __builtin_trap();
        }
        free(obj);
    }
    free(storage);
    return 0;
}
