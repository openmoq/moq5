#ifndef MOQ_LOC_CODEC_STRING_H
#define MOQ_LOC_CODEC_STRING_H

/*
 * Private to the media libraries: the one definition of an acceptable LOC-05
 * Codec String, shared by moq::loc and moq::media-object. Header-only, so no
 * symbol is exported.
 *
 * Two separate checks:
 *   - encoding: well-formed UTF-8 per RFC 3629 section 4 (no overlong forms,
 *     no surrogates, nothing above U+10FFFF, no truncated sequence);
 *   - identifier: not empty, not only ASCII whitespace (U+0009, U+000A,
 *     U+000C, U+000D, U+0020, the WebCodecs stripping set), and no U+0000.
 *     This is the library's minimum for a usable decoder identifier; it is
 *     not part of UTF-8.
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

static inline bool moq_loc_internal_utf8_valid(const uint8_t *p, size_t n)
{
    size_t i = 0;
    while (i < n) {
        uint8_t c = p[i];
        if (c < 0x80) { i++; continue; }
        size_t more;
        uint8_t lo = 0x80, hi = 0xBF;
        if (c >= 0xC2 && c <= 0xDF) {
            more = 1;
        } else if (c >= 0xE0 && c <= 0xEF) {
            more = 2;
            if (c == 0xE0) lo = 0xA0;        /* overlong below U+0800 */
            else if (c == 0xED) hi = 0x9F;   /* surrogates U+D800..DFFF */
        } else if (c >= 0xF0 && c <= 0xF4) {
            more = 3;
            if (c == 0xF0) lo = 0x90;        /* overlong below U+10000 */
            else if (c == 0xF4) hi = 0x8F;   /* above U+10FFFF */
        } else {
            return false;                    /* 80..C1, F5..FF */
        }
        if (more > n - i - 1) return false;
        if (p[i + 1] < lo || p[i + 1] > hi) return false;
        for (size_t k = 2; k <= more; k++)
            if ((p[i + k] & 0xC0u) != 0x80u) return false;
        i += more + 1;
    }
    return true;
}

static inline bool moq_loc_internal_codec_identifier_usable(const uint8_t *p,
                                                            size_t n)
{
    bool non_space = false;
    for (size_t i = 0; i < n; i++) {
        uint8_t c = p[i];
        if (c == 0x00) return false;
        if (c != 0x09 && c != 0x0A && c != 0x0C && c != 0x0D && c != 0x20)
            non_space = true;
    }
    return non_space;
}

static inline bool moq_loc_internal_codec_string_ok(const uint8_t *p, size_t n)
{
    if (n > 0 && !p) return false;
    return moq_loc_internal_codec_identifier_usable(p, n) &&
           moq_loc_internal_utf8_valid(p, n);
}

#endif /* MOQ_LOC_CODEC_STRING_H */
