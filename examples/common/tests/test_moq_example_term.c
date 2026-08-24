/*
 * Unit test for examples/common/moq_example_term.h — the terminal-safe escaper
 * for peer-controlled bytes (security report finding 10). Pure C, no libmoq
 * deps; run under CTest.
 */
#include "../moq_example_term.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;
#define CHECK(cond, msg) do { \
    if (!(cond)) { fprintf(stderr, "FAIL: %s (%s:%d)\n", (msg), __FILE__, __LINE__); \
                   failures++; } \
} while (0)

/* True if `s` contains any byte a terminal would treat as a control: C0
 * (0x00..0x1f), ESC (0x1b, already in C0), DEL (0x7f), or non-ASCII (>=0x80).
 * The escaper's output must never contain any of these. */
static int has_unsafe_byte(const char *s, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        unsigned char b = (unsigned char)s[i];
        if (b < 0x20 || b == 0x7f || b >= 0x80) return 1;
    }
    return 0;
}

static void test_control_bytes_escaped(void)
{
    /* NUL, TAB, LF, CR, ESC, DEL, backslash, plus a printable letter. */
    const uint8_t in[] = { 0x00, 0x09, 0x0a, 0x0d, 0x1b, 0x7f, '\\', 'A' };
    char out[64];
    size_t need = moq_example_term_escape(out, sizeof out, in, sizeof in);

    CHECK(strcmp(out, "\\x00\\t\\n\\r\\x1b\\x7f\\\\A") == 0,
          "control bytes escape to the exact expected string");
    CHECK(need == strlen(out), "return equals full escaped length when untruncated");
    CHECK(!has_unsafe_byte(out, strlen(out)),
          "escaped output contains no unsafe byte");
}

static void test_printable_passthrough(void)
{
    const char *ascii = "video/track_1 (H.264) [main]";
    char out[128];
    size_t need = moq_example_term_escape(out, sizeof out,
                                          (const uint8_t *)ascii, strlen(ascii));
    CHECK(strcmp(out, ascii) == 0, "printable ASCII passes through unchanged");
    CHECK(need == strlen(ascii), "no expansion for printable ASCII");
}

static void test_non_ascii_hex(void)
{
    const uint8_t in[] = { 0xc2, 0xa0, 0xff };   /* non-ASCII => \xNN each */
    char out[32];
    moq_example_term_escape(out, sizeof out, in, sizeof in);
    CHECK(strcmp(out, "\\xc2\\xa0\\xff") == 0,
          "non-ASCII bytes render conservatively as \\xNN");
    CHECK(!has_unsafe_byte(out, strlen(out)), "no raw non-ASCII survives");
}

static void test_truncation_whole_escape(void)
{
    /* A backslash escapes to two chars; with cap for only one, the whole
     * escape is dropped (never split), and the return reports the full need. */
    const uint8_t in[] = { '\\', '\\' };         /* full escape = "\\\\" (4) */
    char out[3];                                 /* room for 2 chars + NUL */
    size_t need = moq_example_term_escape(out, sizeof out, in, sizeof in);
    CHECK(strcmp(out, "\\\\") == 0, "truncates on an escape boundary, no split");
    CHECK(need == 4, "return reports full length so truncation is detectable");
    CHECK(need >= sizeof out, "ret >= dstcap signals truncation");
    CHECK(out[strlen(out)] == '\0', "always NUL-terminated");
}

static void test_zero_cap(void)
{
    const uint8_t in[] = { 'x', 0x1b };
    size_t need = moq_example_term_escape(NULL, 0, in, sizeof in);
    CHECK(need == 1 /*x*/ + 4 /*\x1b*/, "dstcap==0 writes nothing, returns full len");
}

/* Capture the FILE* preview output via open_memstream and assert the format,
 * the full byte count, the truncation marker, and terminal safety. */
static void test_preview_truncation_and_count(void)
{
    char *buf = NULL;
    size_t bufsz = 0;
    FILE *ms = open_memstream(&buf, &bufsz);
    if (!ms) { fprintf(stderr, "open_memstream failed\n"); failures++; return; }

    /* 100 bytes: an ESC, then 99 'A's. Preview cap 8 source bytes. */
    uint8_t payload[100];
    payload[0] = 0x1b;
    for (int i = 1; i < 100; i++) payload[i] = 'A';
    moq_example_term_fprint_preview(ms, payload, sizeof payload, 8);
    fclose(ms);

    /* Full byte count reported; ESC escaped; capped at 8 source bytes (ESC +
     * 7 'A'); truncation marker present. */
    CHECK(strcmp(buf, "100 bytes: \\x1bAAAAAAA[...]") == 0,
          "preview shows full count, escaped bytes, and a truncation marker");
    CHECK(!has_unsafe_byte(buf, strlen(buf)),
          "preview output carries no unsafe byte (peer ESC neutralized)");
    free(buf);
}

static void test_preview_no_truncation_marker_when_short(void)
{
    char *buf = NULL;
    size_t bufsz = 0;
    FILE *ms = open_memstream(&buf, &bufsz);
    if (!ms) { fprintf(stderr, "open_memstream failed\n"); failures++; return; }
    const uint8_t in[] = { 'h', 'i' };
    moq_example_term_fprint_preview(ms, in, sizeof in, 64);
    fclose(ms);
    CHECK(strcmp(buf, "2 bytes: hi") == 0,
          "no truncation marker when len <= cap");
    free(buf);
}

/* (NULL, len>0) must never be dereferenced by any of the three entry points,
 * and must render a printable-ASCII marker carrying the byte count. */
static void test_null_span_escape(void)
{
    char out[64];
    size_t need = moq_example_term_escape(out, sizeof out, NULL, 1234);
    CHECK(strcmp(out, "<null span, 1234 bytes>") == 0,
          "escape: NULL span renders the byte-count marker, no deref");
    CHECK(need == strlen(out), "escape: NULL marker length reported");
    CHECK(!has_unsafe_byte(out, strlen(out)), "escape: NULL marker is safe ASCII");
    CHECK(out[strlen(out)] == '\0', "escape: NUL-terminated on NULL span");
}

static void test_null_span_escape_truncates(void)
{
    /* Whole-escape truncation still holds for the marker path, and `need`
     * still reports the complete marker length. */
    char out[8];
    size_t need = moq_example_term_escape(out, sizeof out, NULL, 1234);
    CHECK(need == strlen("<null span, 1234 bytes>"),
          "escape: NULL span returns full marker length even when truncated");
    CHECK(need >= sizeof out, "escape: ret >= dstcap signals truncation");
    CHECK(out[strlen(out)] == '\0', "escape: truncated NULL marker NUL-terminated");
    CHECK(!has_unsafe_byte(out, strlen(out)), "escape: truncated marker safe");
}

static void test_null_span_fprint(void)
{
    char *buf = NULL; size_t bufsz = 0;
    FILE *ms = open_memstream(&buf, &bufsz);
    if (!ms) { fprintf(stderr, "open_memstream failed\n"); failures++; return; }
    moq_example_term_fprint(ms, NULL, 7);
    fclose(ms);
    CHECK(strcmp(buf, "<null span, 7 bytes>") == 0,
          "fprint: NULL span streams the marker, no deref");
    CHECK(!has_unsafe_byte(buf, strlen(buf)), "fprint: NULL marker safe");
    free(buf);
}

static void test_null_span_preview(void)
{
    char *buf = NULL; size_t bufsz = 0;
    FILE *ms = open_memstream(&buf, &bufsz);
    if (!ms) { fprintf(stderr, "open_memstream failed\n"); failures++; return; }
    moq_example_term_fprint_preview(ms, NULL, 99, 64);
    fclose(ms);
    CHECK(strcmp(buf, "99 bytes: <null>") == 0,
          "preview: NULL span shows count + <null>, no deref");
    CHECK(!has_unsafe_byte(buf, strlen(buf)), "preview: NULL marker safe");
    free(buf);
}

/* A `(NULL, 0)` span is the common empty `{NULL, 0}` moq_bytes_t -- a NORMAL
 * empty span, NOT the malformed anomaly. It must behave exactly like an empty
 * non-null span (no marker). Load-bearing against reverting the guard to
 * `if (src == NULL)`: that would render "<null span, 0 bytes>" here and fail. */
static void test_null_empty_span_escape(void)
{
    char out[32];
    memset(out, 'x', sizeof out);
    size_t need = moq_example_term_escape(out, sizeof out, NULL, 0);
    CHECK(need == 0, "escape: (NULL,0) is empty, returns 0 (no marker)");
    CHECK(strcmp(out, "") == 0, "escape: (NULL,0) writes an empty string");
}

static void test_null_empty_span_fprint(void)
{
    char *buf = NULL; size_t bufsz = 0;
    FILE *ms = open_memstream(&buf, &bufsz);
    if (!ms) { fprintf(stderr, "open_memstream failed\n"); failures++; return; }
    moq_example_term_fprint(ms, NULL, 0);
    fclose(ms);
    CHECK(strcmp(buf, "") == 0, "fprint: (NULL,0) writes nothing (no marker)");
    free(buf);
}

static void test_null_empty_span_preview(void)
{
    char *buf = NULL; size_t bufsz = 0;
    FILE *ms = open_memstream(&buf, &bufsz);
    if (!ms) { fprintf(stderr, "open_memstream failed\n"); failures++; return; }
    moq_example_term_fprint_preview(ms, NULL, 0, 64);
    fclose(ms);
    CHECK(strcmp(buf, "0 bytes: ") == 0,
          "preview: (NULL,0) prints '0 bytes: ' like an empty span (no <null>)");
    free(buf);
}

int main(void)
{
    test_control_bytes_escaped();
    test_printable_passthrough();
    test_non_ascii_hex();
    test_truncation_whole_escape();
    test_zero_cap();
    test_preview_truncation_and_count();
    test_preview_no_truncation_marker_when_short();
    test_null_span_escape();
    test_null_span_escape_truncates();
    test_null_span_fprint();
    test_null_span_preview();
    test_null_empty_span_escape();
    test_null_empty_span_fprint();
    test_null_empty_span_preview();
    if (failures) { fprintf(stderr, "%d check(s) failed\n", failures); return 1; }
    printf("ok\n");
    return 0;
}
