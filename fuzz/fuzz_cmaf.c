/*
 * libFuzzer target for the CMAF segment parser and CMSF object validator
 * (media/cmaf/include/moq/cmaf.h).
 *
 * Invariants under test:
 *   1. moq_cmaf_parse_init():
 *      - Returns only MOQ_OK or MOQ_ERR_PROTO for non-NULL arguments.
 *      - On MOQ_OK, codec_config (and scheme / default_kid when has_cenc)
 *        point within [buf, buf + size], and CENC fields are zeroed when
 *        !has_cenc or exact-width (4 and 16 bytes) when has_cenc.
 *   2. moq_cmaf_parse_fragment() and resize-and-retry security contract:
 *      - Returns only MOQ_OK, MOQ_ERR_PROTO, or MOQ_ERR_BUFFER.
 *      - On MOQ_ERR_PROTO, out.sample_count is always 0 (never leaks an
 *        untrusted trun sample count).
 *      - On MOQ_ERR_BUFFER (tested with a small 4-sample cap),
 *        out.sample_count > 4 and <= size, and retrying with a buffer of
 *        exactly out.sample_count samples succeeds with MOQ_OK and yields
 *        the same sample_count, track_id, chunk_count, and mdat slice.
 *      - On MOQ_OK, mdat points within [buf, buf + size], sample_count >= 1,
 *        chunk_count >= 1, and moq_cmaf_sap_from_sample_flags() never
 *        fabricates exact SAP 1/2/3 from flags alone.
 *   3. moq_cmaf_validate_object():
 *      - Returns only MOQ_OK (valid == true, reason == MOQ_CMAF_OK) or
 *        MOQ_ERR_PROTO (valid == false, reason != MOQ_CMAF_OK).
 *      - Every object accepted by moq_cmaf_validate_object(NULL, ...) is
 *        also accepted by moq_cmaf_parse_fragment() with matching
 *        chunk_count and track_id, and fails with
 *        MOQ_CMAF_ERR_TRACK_ID_MISMATCH when validated against a
 *        non-zero differing init track_id.
 */

#include <moq/cmaf.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define FUZZ_MAX_INPUT 32768

static void check_slice_in(const uint8_t *base, size_t len, moq_bytes_t s)
{
    if (s.len == 0)
        return;
    if (!s.data)
        __builtin_trap();
    uintptr_t b = (uintptr_t)base;
    uintptr_t e = b + len;
    uintptr_t p = (uintptr_t)s.data;
    if (p < b || p > e || s.len > (size_t)(e - p))
        __builtin_trap();
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size > FUZZ_MAX_INPUT)
        return 0;

    /* Copy to a non-NULL heap buffer even when size == 0 so moq_bytes_t.data
     * is non-NULL and ASan catches any 1-byte out-of-bounds read. */
    uint8_t *buf = (uint8_t *)malloc(size > 0 ? size : 1);
    if (!buf)
        return 0;
    if (size > 0)
        memcpy(buf, data, size);
    moq_bytes_t input = { buf, size };

    /* 1. Init segment parser. */
    {
        moq_cmaf_init_info_t init;
        moq_result_t irc = moq_cmaf_parse_init(input, &init);
        if (irc != MOQ_OK && irc != MOQ_ERR_PROTO)
            __builtin_trap();
        if (irc == MOQ_OK) {
            if (init.struct_size != sizeof(init))
                __builtin_trap();
            if (init.codec_kind < MOQ_CMAF_CODEC_UNKNOWN ||
                init.codec_kind > MOQ_CMAF_CODEC_OPUS)
                __builtin_trap();
            check_slice_in(buf, size, init.codec_config);
            if (!init.has_cenc) {
                if (init.scheme.data != NULL || init.scheme.len != 0 ||
                    init.default_kid.data != NULL || init.default_kid.len != 0 ||
                    init.default_is_protected != 0 ||
                    init.default_per_sample_iv_size != 0)
                    __builtin_trap();
            } else {
                if (init.scheme.len != 4 || init.default_kid.len != 16)
                    __builtin_trap();
                check_slice_in(buf, size, init.scheme);
                check_slice_in(buf, size, init.default_kid);
            }
        }
    }

    /* 2. Fragment parser + resize-and-retry contract. */
    {
        moq_cmaf_sample_t small_samples[4];
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, small_samples, 4);
        moq_result_t frc = moq_cmaf_parse_fragment(input, &frag);
        if (frc != MOQ_OK && frc != MOQ_ERR_PROTO && frc != MOQ_ERR_BUFFER)
            __builtin_trap();

        if (frc == MOQ_ERR_PROTO) {
            if (frag.sample_count != 0)
                __builtin_trap();
        } else if (frc == MOQ_OK) {
            if (frag.sample_count == 0 || frag.sample_count > 4 ||
                frag.chunk_count == 0)
                __builtin_trap();
            check_slice_in(buf, size, frag.mdat);
            for (size_t i = 0; i < frag.sample_count; i++) {
                moq_sap_type_t sap =
                    moq_cmaf_sap_from_sample_flags(frag.samples[i].flags);
                if (sap != MOQ_SAP_NONE && sap != MOQ_SAP_UNKNOWN)
                    __builtin_trap();
            }
        } else if (frc == MOQ_ERR_BUFFER) {
            size_t needed = frag.sample_count;
            if (needed <= 4 || needed > size)
                __builtin_trap();
            moq_cmaf_sample_t *dyn = (moq_cmaf_sample_t *)malloc(
                needed * sizeof(moq_cmaf_sample_t));
            if (dyn) {
                moq_cmaf_fragment_info_t frag2;
                moq_cmaf_fragment_info_init(&frag2, dyn, needed);
                if (moq_cmaf_parse_fragment(input, &frag2) != MOQ_OK)
                    __builtin_trap();
                if (frag2.sample_count != needed ||
                    frag2.chunk_count != frag.chunk_count ||
                    frag2.track_id != frag.track_id ||
                    frag2.mdat.data != frag.mdat.data ||
                    frag2.mdat.len != frag.mdat.len)
                    __builtin_trap();
                check_slice_in(buf, size, frag2.mdat);
                for (size_t i = 0; i < frag2.sample_count; i++) {
                    moq_sap_type_t sap =
                        moq_cmaf_sap_from_sample_flags(frag2.samples[i].flags);
                    if (sap != MOQ_SAP_NONE && sap != MOQ_SAP_UNKNOWN)
                        __builtin_trap();
                }
                free(dyn);
            }
        }
    }

    /* 3. CMSF §3.3 object packaging validator + cross-check with fragment. */
    {
        moq_cmaf_object_report_t rep;
        moq_result_t vrc = moq_cmaf_validate_object(NULL, input, &rep);
        if (vrc != MOQ_OK && vrc != MOQ_ERR_PROTO)
            __builtin_trap();
        if (rep.struct_size != sizeof(rep))
            __builtin_trap();

        if (vrc == MOQ_OK) {
            if (!rep.valid || rep.reason != MOQ_CMAF_OK ||
                rep.chunk_count == 0)
                __builtin_trap();
            if (rep.sap_type != MOQ_SAP_NONE &&
                rep.sap_type != MOQ_SAP_UNKNOWN)
                __builtin_trap();

            moq_cmaf_sample_t sbuf[512];
            moq_cmaf_fragment_info_t f512;
            moq_cmaf_fragment_info_init(&f512, sbuf, 512);
            if (moq_cmaf_parse_fragment(input, &f512) != MOQ_OK ||
                f512.chunk_count != rep.chunk_count ||
                f512.track_id != rep.track_id)
                __builtin_trap();

            if (rep.track_id != 0) {
                moq_cmaf_init_info_t match_init;
                moq_cmaf_init_info_init(&match_init);
                match_init.track_id = rep.track_id;
                moq_cmaf_object_report_t mrep;
                if (moq_cmaf_validate_object(&match_init, input,
                                             &mrep) != MOQ_OK ||
                    !mrep.valid)
                    __builtin_trap();

                moq_cmaf_init_info_t mismatch_init;
                moq_cmaf_init_info_init(&mismatch_init);
                mismatch_init.track_id =
                    (rep.track_id == 1) ? 2u : 1u;
                moq_cmaf_object_report_t xrep;
                if (moq_cmaf_validate_object(&mismatch_init, input,
                                             &xrep) != MOQ_ERR_PROTO ||
                    xrep.valid ||
                    xrep.reason != MOQ_CMAF_ERR_TRACK_ID_MISMATCH)
                    __builtin_trap();
            }
        } else {
            if (rep.valid || rep.reason == MOQ_CMAF_OK ||
                rep.reason < MOQ_CMAF_ERR_NO_CHUNK ||
                rep.reason > MOQ_CMAF_ERR_NO_SAMPLES)
                __builtin_trap();
        }
    }

    free(buf);
    return 0;
}
