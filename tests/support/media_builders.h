#ifndef MOQ_TEST_MEDIA_BUILDERS_H
#define MOQ_TEST_MEDIA_BUILDERS_H

/*
 * Shared deterministic ISO-BMFF / CMAF object builders for media-format
 * conformance tests. Seeded from media/cmaf/tests/test_cmaf.c (the proven
 * builders the CMAF validator is tested against); this is the eventual home
 * once the per-format tests are deduped onto it (tracked fast-follow).
 *
 * Test-only support: compiled directly into the consuming test executable
 * (the tests/support convention), never part of a production/installed target.
 * Only the builders the conformance test actually calls live here -- no
 * "for future use" helpers (they would trip -Wunused-function -Werror).
 */

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/* Write a box header [size:4 BE][type:4] at p; returns 8. */
size_t moq_test_box_hdr(uint8_t *p, uint32_t size, const char *type);

/*
 * One CMAF chunk: moof( mfhd? traf[+traf] ( tfhd tfdt trun ) ) + mdat(1 byte).
 *   include_mfhd=false omits the mandatory mfhd (CMSF §3.3 violation);
 *   two_traf=true adds a second track fragment (multi-track violation);
 *   first_sample_flags drives the chunk's SAP classification.
 * Returns the total byte length written to out.
 */
size_t moq_test_build_cmaf_chunk(uint8_t *out, uint32_t track_id,
                                 uint32_t first_sample_flags,
                                 bool include_mfhd, bool two_traf,
                                 uint8_t mdat_byte);

/*
 * A structurally well-formed chunk whose trun declares zero samples
 * (moof(mfhd traf(tfhd tfdt trun[count=0])) + mdat). Drives NO_SAMPLES.
 */
size_t moq_test_build_cmaf_chunk_no_samples(uint8_t *out, uint32_t track_id,
                                            uint8_t mdat_byte);

/*
 * Omit/corrupt toggles for the CENC init builder below -- each drops or
 * truncates one required protection box to drive a negative parse
 * (MOQ_ERR_PROTO). MOQ_TEST_CENC_OK builds a well-formed protected init.
 */
enum {
    MOQ_TEST_CENC_OK           = 0,
    MOQ_TEST_CENC_OMIT_SINF    = 1u << 0,
    MOQ_TEST_CENC_OMIT_FRMA    = 1u << 1,
    MOQ_TEST_CENC_OMIT_SCHM    = 1u << 2,
    MOQ_TEST_CENC_OMIT_SCHI    = 1u << 3,
    MOQ_TEST_CENC_OMIT_TENC    = 1u << 4,
    MOQ_TEST_CENC_SHORT_TENC   = 1u << 5,  /* tenc body < 23 bytes */
    MOQ_TEST_CENC_SHORT_SCHM   = 1u << 6,  /* schm body 8 (< hdr+12) */
    MOQ_TEST_CENC_TRAILING_TENC = 1u << 7, /* tenc body 27 (23 + trailing) */
    MOQ_TEST_CENC_OMIT_AVCC    = 1u << 8,  /* drop the inner avcC config box */
    MOQ_TEST_CENC_FRMA_UNKNOWN = 1u << 9,  /* frma = "zzzz" (unknown codec) */
};

/*
 * Build a CENC-protected AVC CMAF init segment:
 *   ftyp + moov(trak(tkhd mdia(mdhd minf(stbl(stsd(
 *       encv(<VisualSampleEntry> avcC sinf(frma schm schi(tenc)))))))))
 * The encv sample entry carries the original format ('avc1') in frma, the
 * scheme 4cc (e.g. "cenc"/"cbcs") in schm, and default_isProtected / IV size /
 * default_KID in a tenc of the given version (0 or 1; identical field offsets).
 * `omit` (above) drops/truncates boxes for negatives. Returns bytes written.
 * Test builder: pass a small avcC (fits the internal layout assumptions).
 */
size_t moq_test_build_cmaf_init_cenc(uint8_t *out, uint32_t track_id,
                                     uint32_t timescale,
                                     uint16_t w, uint16_t h,
                                     const uint8_t *avcc, size_t avcc_len,
                                     const char scheme[4],
                                     uint8_t default_is_protected,
                                     uint8_t iv_size, const uint8_t kid[16],
                                     uint8_t tenc_version, uint32_t omit);

#endif /* MOQ_TEST_MEDIA_BUILDERS_H */
