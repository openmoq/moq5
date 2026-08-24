/*
 * moq_example_term.h — terminal-safe rendering of PEER-CONTROLLED bytes for
 * LibMoQ example programs. Example support ONLY: header-only, not installed,
 * not part of any public API.
 *
 * A peer controls track names, codec strings, content-protection identifiers,
 * schemes, and object payloads. Printing those raw lets a remote peer inject
 * ANSI/OSC escape sequences, carriage returns, and other control bytes into the
 * operator's terminal (log spoofing, cursor/clipboard/title abuse). `%.*s`
 * bounds the read but does not neutralize the bytes.
 *
 * These helpers are length-aware (they take byte spans, never C strings) and
 * escape every byte that is not printable 7-bit ASCII:
 *   - '\\'                      -> "\\"          (so escapes are unambiguous)
 *   - '\n' / '\r' / '\t'        -> "\\n"/"\\r"/"\\t"
 *   - any other 0x00..0x1f      -> "\\xNN"       (C0 controls, includes ESC/BEL)
 *   - 0x7f (DEL)                -> "\\x7f"
 *   - 0x80..0xff (non-ASCII)    -> "\\xNN"
 *   - printable ASCII 0x20..0x7e (except '\\') -> passthrough
 *
 * NON-ASCII POLICY (deliberate): non-ASCII bytes are rendered conservatively as
 * "\\xNN" rather than passed through as UTF-8. An example must be safe without a
 * UTF-8 validator; a lone 0xC2, an overlong form, or a byte order mark can carry
 * or mask a terminal-control effect, so nothing above 0x7e is emitted raw. The
 * only bytes that ever reach the terminal from these helpers are 0x20..0x7e.
 */
#ifndef MOQ_EXAMPLE_TERM_H
#define MOQ_EXAMPLE_TERM_H

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

/* Render one byte into `out` (which must hold at least 5 bytes: up to "\xNN"
 * plus a NUL). Returns the number of characters written (1..4), NOT counting
 * the NUL. Never emits a raw control, ESC, DEL, backslash, or non-ASCII byte. */
static inline size_t moq_example_escape_byte(unsigned char b, char out[5])
{
    static const char hex[] = "0123456789abcdef";
    switch (b) {
    case '\\': out[0] = '\\'; out[1] = '\\'; out[2] = '\0'; return 2;
    case '\n': out[0] = '\\'; out[1] = 'n';  out[2] = '\0'; return 2;
    case '\r': out[0] = '\\'; out[1] = 'r';  out[2] = '\0'; return 2;
    case '\t': out[0] = '\\'; out[1] = 't';  out[2] = '\0'; return 2;
    default: break;
    }
    if (b >= 0x20 && b <= 0x7e) {      /* printable ASCII (backslash handled) */
        out[0] = (char)b; out[1] = '\0'; return 1;
    }
    out[0] = '\\'; out[1] = 'x';
    out[2] = hex[(b >> 4) & 0xf];
    out[3] = hex[b & 0xf];
    out[4] = '\0';
    return 4;
}

/* Format the printable-ASCII marker used when a source pointer is NULL but the
 * length is non-zero (a malformed/unexpected span). Includes the byte count so
 * the anomaly is visible. Returns the marker length (excluding the NUL). */
static inline size_t moq_example__null_marker(char *buf, size_t bufcap,
                                              size_t len)
{
    int w = snprintf(buf, bufcap, "<null span, %zu bytes>", len);
    return (w < 0) ? 0 : (size_t)w;
}

/* Internal: escape span[0..n) into dst (the NULL-safe callers pass either the
 * real span or the marker). Saturating length accounting -- never wraps. */
static inline size_t moq_example__escape_span(char *dst, size_t dstcap,
                                              const uint8_t *span, size_t n)
{
    size_t need = 0;                   /* total output chars the full escape needs */
    size_t out = 0;                    /* chars actually placed in dst */
    for (size_t i = 0; i < n; i++) {
        char tmp[5];
        size_t w = moq_example_escape_byte((unsigned char)span[i], tmp);
        need = (need > SIZE_MAX - w) ? SIZE_MAX : need + w;   /* saturate */
        if (dstcap > 0 && out + w <= dstcap - 1) {
            for (size_t k = 0; k < w; k++) dst[out + k] = tmp[k];
            out += w;
        }
    }
    if (dstcap > 0) dst[out] = '\0';
    return need;
}

/* Internal: escape span[0..n) directly to `f` (streaming). */
static inline void moq_example__fprint_span(FILE *f,
                                            const uint8_t *span, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        char tmp[5];
        moq_example_escape_byte((unsigned char)span[i], tmp);
        fputs(tmp, f);
    }
}

/*
 * Escape src[0..len) into dst as a terminal-safe, NUL-terminated C string.
 * At most (dstcap - 1) characters are written and dst is always NUL-terminated
 * when dstcap > 0 (a partial escape at the boundary is dropped whole, never
 * split). Returns the total number of characters the COMPLETE escaping would
 * produce (excluding the NUL) -- so `ret >= dstcap` means the output was
 * truncated. dstcap == 0 writes nothing and still returns the full length.
 *
 * NULL-safe: a `(src == NULL, len > 0)` span is a malformed anomaly -- it is
 * NEVER dereferenced and renders a printable-ASCII marker including the byte
 * count, with the same return and truncation semantics (the marker is all
 * printable ASCII, so it escapes to itself). A `(src == NULL, len == 0)` span
 * is a NORMAL empty span (the common `{NULL, 0}` `moq_bytes_t`): it writes `""`
 * and returns 0, exactly like an empty non-null span.
 */
static inline size_t moq_example_term_escape(char *dst, size_t dstcap,
                                             const uint8_t *src, size_t len)
{
    if (src == NULL && len > 0) {
        char m[64];
        size_t ml = moq_example__null_marker(m, sizeof m, len);
        return moq_example__escape_span(dst, dstcap, (const uint8_t *)m, ml);
    }
    return moq_example__escape_span(dst, dstcap, src, len);
}

/* Escape src[0..len) directly to `f`, streaming (no allocation, no length
 * bound). Use for SHORT, already-bounded metadata (names, codecs, ids).
 * NULL-safe: `(NULL, len > 0)` renders the printable-ASCII marker instead of
 * dereferencing; `(NULL, 0)` is a normal empty span and writes nothing. */
static inline void moq_example_term_fprint(FILE *f,
                                           const uint8_t *src, size_t len)
{
    if (src == NULL && len > 0) {
        char m[64];
        size_t ml = moq_example__null_marker(m, sizeof m, len);
        moq_example__fprint_span(f, (const uint8_t *)m, ml);
        return;
    }
    moq_example__fprint_span(f, src, len);
}

/*
 * Bounded preview of an ARBITRARY payload: prints the full byte count, then an
 * escaped preview of at most `max_preview` SOURCE bytes, and a truncation
 * marker when len > max_preview. Format:  "<len> bytes: <escaped>[...]".
 * The output-char count stays bounded because the source span is capped first.
 * NULL-safe: a `(src == NULL, len > 0)` span prints "<len> bytes: <null>" and
 * dereferences nothing; a `(src == NULL, 0)` span is a normal empty payload and
 * prints "0 bytes: ", exactly like an empty non-null span.
 */
static inline void moq_example_term_fprint_preview(FILE *f,
                                                   const uint8_t *src,
                                                   size_t len,
                                                   size_t max_preview)
{
    fprintf(f, "%zu bytes: ", len);
    if (src == NULL && len > 0) { fputs("<null>", f); return; }
    size_t shown = len < max_preview ? len : max_preview;
    moq_example__fprint_span(f, src, shown);
    if (len > shown) fputs("[...]", f);
}

#endif /* MOQ_EXAMPLE_TERM_H */
