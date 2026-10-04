/*
 * Draft-21 control-message codec tests (draft-ietf-moq-transport-21).
 *
 * Every vector here is derived from the draft-21 text and the in-repo wire
 * reference (docs/draft21-wire-reference.md), not from the draft-18 codec this
 * one started as. Each section names the draft-21 section it pins. Sections are
 * added one message family at a time as the family is converted.
 *
 * Message framing is Type (vi64) + Length (16, big endian) + body. SETUP is type
 * 0x2F00, which is the two-byte vi64 form AF 00.
 */
#include <moq/moq.h>
#include <moq/control_d21.h>
#include <moq/buf.h>
#include "test_support.h"
#include <string.h>

static int failures = 0;

/* Compare an encoded buffer to an expected byte vector, with a readable failure. */
static void check_bytes(const char *what, const uint8_t *got, size_t got_len,
                        const uint8_t *want, size_t want_len)
{
    if (got_len != want_len || memcmp(got, want, want_len) != 0) {
        fprintf(stderr, "FAIL: %s: bytes differ (got %zu, want %zu)\n  got : ",
                what, got_len, want_len);
        for (size_t i = 0; i < got_len; i++) fprintf(stderr, "%02x ", got[i]);
        fprintf(stderr, "\n  want: ");
        for (size_t i = 0; i < want_len; i++) fprintf(stderr, "%02x ", want[i]);
        fprintf(stderr, "\n");
        failures++;
    }
}

/* == 4a. SETUP options (draft-21 section 9.1) ============================== */

static void t_setup_options_encode(void)
{
    /* 9.1.6: MAX_FILTER_RANGES is type 0x06 (even, a single vi64 value). */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d21_setup_opts_t o;
        memset(&o, 0, sizeof(o));
        o.has_max_filter_ranges = true;
        o.max_filter_ranges = 3;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_setup_opts(&w, &o), (int)MOQ_OK);
        static const uint8_t want[] = { 0xAF, 0x00, 0x00, 0x02, 0x06, 0x03 };
        check_bytes("SETUP MAX_FILTER_RANGES=3", buf, moq_buf_writer_offset(&w),
                    want, sizeof(want));
    }

    /* All four sourced options, in ascending type order with delta types:
     * 0x04 (absolute), 0x06 (+2), 0x07 (+1, odd: length + bytes), 0x08 (+1).
     * 9.1.3, 9.1.6, 9.1.5, 9.1.7. */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d21_setup_opts_t o;
        memset(&o, 0, sizeof(o));
        o.has_max_auth_token_cache_size = true;
        o.max_auth_token_cache_size = 100;
        o.has_max_filter_ranges = true;
        o.max_filter_ranges = 1;
        o.has_implementation = true;
        o.implementation = MOQ_BYTES_LITERAL("moq5/0.1");
        o.has_max_request_updates = true;
        o.max_request_updates = 2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_setup_opts(&w, &o), (int)MOQ_OK);
        static const uint8_t want[] = {
            0xAF, 0x00, 0x00, 0x10,
            0x04, 0x64,                                   /* cache size 100 */
            0x02, 0x01,                                   /* +2 -> 0x06 = 1 */
            0x01, 0x08, 'm', 'o', 'q', '5', '/', '0', '.', '1', /* +1 -> 0x07 */
            0x01, 0x02                                    /* +1 -> 0x08 = 2 */
        };
        check_bytes("SETUP all options", buf, moq_buf_writer_offset(&w), want,
                    sizeof(want));
    }

    /* MAX_REQUEST_UPDATES alone is absolute type 0x08. */
    {
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d21_setup_opts_t o;
        memset(&o, 0, sizeof(o));
        o.has_max_request_updates = true;
        o.max_request_updates = 0;     /* 0 = unlimited, still sent when set */
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_setup_opts(&w, &o), (int)MOQ_OK);
        static const uint8_t want[] = { 0xAF, 0x00, 0x00, 0x02, 0x08, 0x00 };
        check_bytes("SETUP MAX_REQUEST_UPDATES=0", buf, moq_buf_writer_offset(&w),
                    want, sizeof(want));
    }

    /* No options is a zero-length body. */
    {
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_setup_opts(&w, NULL), (int)MOQ_OK);
        static const uint8_t want[] = { 0xAF, 0x00, 0x00, 0x00 };
        check_bytes("SETUP no options", buf, moq_buf_writer_offset(&w), want,
                    sizeof(want));
    }

    /* A value that cannot be framed is refused, not truncated: a KVP length is
     * at most 2^16-1 (8.3). */
    {
        static uint8_t big[0x10000];
        static uint8_t buf[0x10000 + 64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d21_setup_opts_t o;
        memset(&o, 0, sizeof(o));
        o.has_implementation = true;
        o.implementation.data = big;
        o.implementation.len = sizeof(big);
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_setup_opts(&w, &o),
                              (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);   /* nothing left behind */
    }
}

static void t_setup_options_decode(void)
{
    /* Round trip of the all-options vector. */
    {
        static const uint8_t p[] = {
            0x04, 0x64, 0x02, 0x01,
            0x01, 0x08, 'm', 'o', 'q', '5', '/', '0', '.', '1',
            0x01, 0x02
        };
        moq_d21_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_setup_opts(p, sizeof(p), &o),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(o.has_max_auth_token_cache_size);
        MOQ_TEST_CHECK_EQ_U64(o.max_auth_token_cache_size, 100);
        MOQ_TEST_CHECK(o.has_max_filter_ranges);
        MOQ_TEST_CHECK_EQ_U64(o.max_filter_ranges, 1);
        MOQ_TEST_CHECK(o.has_implementation);
        MOQ_TEST_CHECK_EQ_SIZE(o.implementation.len, 8);
        MOQ_TEST_CHECK(o.implementation.len == 8 &&
                       memcmp(o.implementation.data, "moq5/0.1", 8) == 0);
        MOQ_TEST_CHECK(o.has_max_request_updates);
        MOQ_TEST_CHECK_EQ_U64(o.max_request_updates, 2);
    }

    /* Absent options read as absent, so the draft's defaults apply (9.1.3: cache
     * size 0; 9.1.6: MAX_FILTER_RANGES 0; 9.1.7: MAX_REQUEST_UPDATES 0). */
    {
        moq_d21_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_setup_opts(NULL, 0, &o),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(!o.has_max_filter_ranges && !o.has_max_request_updates &&
                       !o.has_implementation && !o.has_max_auth_token_cache_size);
        MOQ_TEST_CHECK_EQ_U64(o.max_filter_ranges, 0);
        MOQ_TEST_CHECK_EQ_U64(o.max_request_updates, 0);
    }

    /* The full 64-bit range is a valid value (vi64, 8.1): 0x06 = 2^64-1. */
    {
        static const uint8_t p[] = {
            0x06, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF
        };
        moq_d21_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_setup_opts(p, sizeof(p), &o),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(o.max_filter_ranges, UINT64_MAX);
    }

    /* Non-minimal vi64 values are valid and decode to the same number (8.1). */
    {
        static const uint8_t p[] = { 0x08, 0x80, 0x02 };   /* 2 as a 2-byte vi64 */
        moq_d21_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_setup_opts(p, sizeof(p), &o),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(o.max_request_updates, 2);
    }

    /* Unknown options, including GREASE ids (13: 0x7f*N + 0x9D = 0x9D, 0x11C),
     * are ignored whatever their parity, and unknown duplicates are allowed
     * (9.1). 0x9D is odd (length-prefixed); 0x11C is even (a single vi64). */
    {
        uint8_t p[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        moq_buf_write_vi64(&w, 0x06); moq_buf_write_vi64(&w, 5);   /* known */
        moq_buf_write_vi64(&w, 0x9D - 0x06);                       /* GREASE odd */
        moq_buf_write_vi64(&w, 2);
        moq_buf_write_raw(&w, (const uint8_t *)"zz", 2);
        moq_buf_write_vi64(&w, 0);                                 /* duplicate */
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_raw(&w, (const uint8_t *)"y", 1);
        moq_buf_write_vi64(&w, 0x11C - 0x9D);                      /* GREASE even */
        moq_buf_write_vi64(&w, 77);
        moq_d21_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d21_decode_setup_opts(p, moq_buf_writer_offset(&w), &o),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(o.max_filter_ranges, 5);
    }

    /* A known option may not repeat (9.1: a sender MUST NOT repeat an option
     * type); a receiver closes with PROTOCOL_VIOLATION. */
    {
        static const uint8_t dup_filter[]  = { 0x06, 0x01, 0x00, 0x02 };
        static const uint8_t dup_updates[] = { 0x08, 0x01, 0x00, 0x02 };
        static const uint8_t dup_impl[]    = { 0x07, 0x01, 'a', 0x00, 0x01, 'b' };
        moq_d21_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d21_decode_setup_opts(dup_filter, sizeof(dup_filter), &o),
            (int)MOQ_ERR_PROTO);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d21_decode_setup_opts(dup_updates, sizeof(dup_updates), &o),
            (int)MOQ_ERR_PROTO);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d21_decode_setup_opts(dup_impl, sizeof(dup_impl), &o),
            (int)MOQ_ERR_PROTO);
    }

    /* A length above 2^16-1 closes with PROTOCOL_VIOLATION (8.3); a length that
     * runs past the message does too. 65536 as a 3-byte vi64 is C1 00 00. */
    {
        static const uint8_t over[]  = { 0x07, 0xC1, 0x00, 0x00 };
        static const uint8_t short_[] = { 0x07, 0x05, 'a', 'b' };
        moq_d21_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d21_decode_setup_opts(over, sizeof(over), &o),
            (int)MOQ_ERR_PROTO);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d21_decode_setup_opts(short_, sizeof(short_), &o),
            (int)MOQ_ERR_PROTO);
    }

    /* A truncated value is a protocol violation, and a Delta Type that would
     * overflow 2^64-1 is too. */
    {
        static const uint8_t trunc[] = { 0x06 };           /* type, no value */
        uint8_t ovf[24];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ovf, sizeof(ovf));
        moq_buf_write_vi64(&w, 0x06); moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, UINT64_MAX);                /* 6 + (2^64-1) overflows */
        moq_buf_write_vi64(&w, 0);
        moq_d21_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d21_decode_setup_opts(trunc, sizeof(trunc), &o),
            (int)MOQ_ERR_PROTO);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d21_decode_setup_opts(ovf, moq_buf_writer_offset(&w), &o),
            (int)MOQ_ERR_PROTO);
    }
}

int main(void)
{
    t_setup_options_encode();
    t_setup_options_decode();

    if (failures)
        fprintf(stderr, "test_control_d21: %d byte-vector failures\n", failures);
    MOQ_TEST_PASS("test_control_d21");
}
