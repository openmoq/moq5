/* The bounded, validating JSON writer: every value it is given either
 * validates and lands whole, or the document is refused. Nothing here reads
 * a clock or touches I/O. */

#include <json_out.h>

#include <stdio.h>
#include <string.h>

#include "../../../tests/unit/test_support.h"

/* Render one string value as a whole document and return the body. */
static bool
render_str(const char *s, size_t n, char *out, size_t cap, size_t *len)
{
    moqr_json_w_t w;
    moqr_json_begin(&w, out, cap);
    moqr_json_str(&w, s, n);
    return moqr_json_end(&w, len);
}

static int
expect_str(const char *in, size_t n, const char *want)
{
    char out[256];
    size_t len = 0;
    if (!render_str(in, n, out, sizeof(out), &len)) {
        printf("  refused a valid string (%zu bytes)\n", n);
        return 1;
    }
    if (len != strlen(want) || strcmp(out, want) != 0) {
        printf("  rendered [%s], want [%s]\n", out, want);
        return 1;
    }
    return 0;
}

static int
expect_refused(const char *what, const char *in, size_t n)
{
    char out[256];
    size_t len = 7;
    memset(out, 'x', sizeof(out));
    if (render_str(in, n, out, sizeof(out), &len)) {
        printf("  accepted %s\n", what);
        return 1;
    }
    if (out[0] != '\0' || len != 0) {
        printf("  refusal of %s left a partial document behind\n", what);
        return 1;
    }
    return 0;
}

static int
test_escaping(void)
{
    int failures = 0;
    /* Plain ASCII passes through; quote and backslash escape. */
    failures += expect_str("abc", 3, "\"abc\"");
    failures += expect_str("a\"b", 3, "\"a\\\"b\"");
    failures += expect_str("a\\b", 3, "\"a\\\\b\"");
    /* Every control byte below 0x20 and 0x7F escapes as \u00XX. */
    for (unsigned c = 0; c < 0x20u; c++) {
        char in[1] = { (char)c };
        char want[16];
        (void)snprintf(want, sizeof(want), "\"\\u%04x\"", c);
        failures += expect_str(in, 1, want);
    }
    failures += expect_str("\x7f", 1, "\"\\u007f\"");
    /* Valid 2-, 3- and 4-byte sequences are copied unchanged. */
    failures += expect_str("\xc3\xa9", 2, "\"\xc3\xa9\"");
    failures += expect_str("\xe2\x82\xac", 3, "\"\xe2\x82\xac\"");
    failures += expect_str("\xf0\x9f\x98\x80", 4, "\"\xf0\x9f\x98\x80\"");
    failures += expect_str("\xef\xbf\xbf", 3, "\"\xef\xbf\xbf\"");       /* U+FFFF */
    failures += expect_str("\xf4\x8f\xbf\xbf", 4, "\"\xf4\x8f\xbf\xbf\""); /* U+10FFFF */
    failures += expect_str("\xed\x9f\xbf", 3, "\"\xed\x9f\xbf\"");       /* U+D7FF */
    failures += expect_str("\xee\x80\x80", 3, "\"\xee\x80\x80\"");       /* U+E000 */
    failures += expect_str("", 0, "\"\"");
    /* An embedded NUL is a control byte, not a terminator. */
    failures += expect_str("a\0b", 3, "\"a\\u0000b\"");
    return failures;
}

static int
test_utf8_refusals(void)
{
    int failures = 0;
    failures += expect_refused("a raw 0xff", "\xff", 1);
    failures += expect_refused("an isolated continuation byte", "\x80", 1);
    failures += expect_refused("a continuation inside ASCII", "a\xbfz", 3);
    failures += expect_refused("a truncated 2-byte sequence", "\xc3", 1);
    failures += expect_refused("a truncated 3-byte sequence at the end",
                               "ab\xe2\x82", 4);
    failures += expect_refused("a truncated 4-byte sequence", "\xf0\x9f\x98", 3);
    failures += expect_refused("an overlong 2-byte NUL", "\xc0\x80", 2);
    failures += expect_refused("an overlong 2-byte form", "\xc1\xbf", 2);
    failures += expect_refused("an overlong 3-byte form", "\xe0\x80\x80", 3);
    failures += expect_refused("an overlong 4-byte form", "\xf0\x80\x80\x80", 4);
    failures += expect_refused("an encoded surrogate", "\xed\xa0\x80", 3);
    failures += expect_refused("an encoded low surrogate", "\xed\xbf\xbf", 3);
    failures += expect_refused("a value above U+10FFFF", "\xf4\x90\x80\x80", 4);
    failures += expect_refused("a 5-byte lead", "\xf8\x88\x80\x80\x80", 5);
    failures += expect_refused("a bad second byte", "\xe2\x28\xa1", 3);
    /* The validation rule itself. */
    MOQ_TEST_CHECK(moqr_json_utf8_valid("ok\xc3\xa9", 4));
    MOQ_TEST_CHECK(!moqr_json_utf8_valid("\xc3", 1));
    MOQ_TEST_CHECK(moqr_json_utf8_valid(NULL, 0));
    MOQ_TEST_CHECK(!moqr_json_utf8_valid(NULL, 1));
    return failures;
}

static int
test_bounded_arrays(void)
{
    int failures = 0;
    char out[64];
    size_t len = 0;
    moqr_json_w_t w;
    char arr[4] = { 'a', 'b', 'c', 'd' };   /* no terminator inside */
    char ok[4] = { 'a', 'b', '\0', 'd' };

    moqr_json_begin(&w, out, sizeof(out));
    moqr_json_str_bounded(&w, arr, sizeof(arr));
    if (moqr_json_end(&w, &len)) {
        printf("  an unterminated array was scanned past and accepted\n");
        failures++;
    }
    moqr_json_begin(&w, out, sizeof(out));
    moqr_json_str_bounded(&w, ok, sizeof(ok));
    if (!moqr_json_end(&w, &len) || strcmp(out, "\"ab\"") != 0) {
        printf("  a terminated array was not read up to its terminator\n");
        failures++;
    }
    return failures;
}

static int
test_structure_and_numbers(void)
{
    int failures = 0;
    char out[256];
    size_t len = 0;
    moqr_json_w_t w;
    static const char *want =
        "{\"a\":1,\"b\":[true,false,\"x\"],\"c\":{\"d\":-5,\"e\":"
        "18446744073709551615},\"f\":[]}";

    moqr_json_begin(&w, out, sizeof(out));
    moqr_json_object_begin(&w);
    moqr_json_key(&w, "a"); moqr_json_u64(&w, 1);
    moqr_json_key(&w, "b"); moqr_json_array_begin(&w);
    moqr_json_bool(&w, true); moqr_json_bool(&w, false);
    moqr_json_str(&w, "x", 1);
    moqr_json_array_end(&w);
    moqr_json_key(&w, "c"); moqr_json_object_begin(&w);
    moqr_json_key(&w, "d"); moqr_json_i64(&w, -5);
    moqr_json_key(&w, "e"); moqr_json_u64(&w, UINT64_MAX);
    moqr_json_object_end(&w);
    moqr_json_key(&w, "f"); moqr_json_array_begin(&w); moqr_json_array_end(&w);
    moqr_json_object_end(&w);
    if (!moqr_json_end(&w, &len) || strcmp(out, want) != 0 ||
        len != strlen(want)) {
        printf("  structure rendered [%s]\n  want               [%s]\n", out,
               want);
        failures++;
    }
    /* An unclosed container is not a document. */
    moqr_json_begin(&w, out, sizeof(out));
    moqr_json_object_begin(&w);
    if (moqr_json_end(&w, &len)) {
        printf("  an unclosed object was accepted\n");
        failures++;
    }
    /* Too deep is refused, never overrun. */
    moqr_json_begin(&w, out, sizeof(out));
    for (unsigned d = 0; d < MOQR_JSON_MAX_DEPTH + 2u; d++) {
        moqr_json_array_begin(&w);
    }
    for (unsigned d = 0; d < MOQR_JSON_MAX_DEPTH + 2u; d++) {
        moqr_json_array_end(&w);
    }
    if (moqr_json_end(&w, &len)) {
        printf("  a document deeper than the bound was accepted\n");
        failures++;
    }
    return failures;
}

/* cap = N + 1 fits an N-byte body; cap = N refuses; more room never breaks
 * a document that fitted; and the counting writer measures exactly N. */
static int
test_exact_fit(void)
{
    int failures = 0;
    static const char *in = "a\"\x01\xc3\xa9";   /* 1 + 2 + 6 + 2 = 11, +2 quotes */
    const size_t n = 13;
    char out[64];
    size_t len = 0;
    moqr_json_w_t w;

    if (!render_str(in, 5, out, n + 1u, &len) || len != n) {
        printf("  cap = N + 1 did not fit an N-byte body (len %zu)\n", len);
        failures++;
    }
    if (render_str(in, 5, out, n, &len)) {
        printf("  cap = N accepted an N-byte body (no room for the NUL)\n");
        failures++;
    }
    if (!render_str(in, 5, out, n + 7u, &len) || len != n) {
        printf("  extra capacity broke a document that fitted\n");
        failures++;
    }
    moqr_json_begin(&w, NULL, 0);
    moqr_json_str(&w, in, 5);
    if (!moqr_json_end(&w, &len) || len != n) {
        printf("  the counting writer measured %zu, not %zu\n", len, n);
        failures++;
    }
    /* Expansion is bounded at six per byte, and the bound saturates. */
    if (moqr_json_escaped_max(10) != 60u ||
        moqr_json_escaped_max(SIZE_MAX) != SIZE_MAX) {
        printf("  the escape expansion bound is wrong\n");
        failures++;
    }
    /* A zero-capacity buffer is refused, not written. */
    moqr_json_begin(&w, out, 0);
    moqr_json_bool(&w, true);
    if (moqr_json_end(&w, &len)) {
        printf("  a zero-capacity buffer was written\n");
        failures++;
    }
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_escaping();
    failures += test_utf8_refusals();
    failures += test_bounded_arrays();
    failures += test_structure_and_numbers();
    failures += test_exact_fit();
    if (failures != 0) {
        printf("FAIL: %d json writer violation(s)\n", failures);
        return 1;
    }
    printf("PASS: relay_admin_json\n");
    return 0;
}
