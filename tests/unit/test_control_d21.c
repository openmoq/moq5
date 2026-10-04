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


/* == 4b. Message parameters (draft-21 section 9.20) ======================== */

/* Decode a parameter block: a vi64 count then that many parameters. Positive
 * tests require the block to be consumed exactly. */
static moq_result_t decode_block(const uint8_t *b, size_t n, uint32_t mask,
                                 moq_d21_msg_params_t *out)
{
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, b, n);
    uint64_t count;
    if (moq_buf_read_vi64(&r, &count) < 0) return MOQ_ERR_BUFFER;
    moq_result_t rc = moq_d21_decode_msg_params(&r, count, mask, out);
    if (rc == MOQ_OK && moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return rc;
}

#define ALL_BITS 0xFFFFFFFFu

static void encode_params_expect(const char *what, const moq_d21_msg_params_t *p,
                                 const uint8_t *want, size_t want_len)
{
    uint8_t buf[256];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_msg_params(&w, p), (int)MOQ_OK);
    check_bytes(what, buf, moq_buf_writer_offset(&w), want, want_len);
}

static void t_params_encode_vectors(void)
{
    moq_d21_msg_params_t p;

    /* 9.20.19: FORWARD is a raw uint8 under type 0x10. */
    memset(&p, 0, sizeof(p));
    p.has_forward = true; p.forward = 0;
    { static const uint8_t want[] = { 0x01, 0x10, 0x00 };
      encode_params_expect("FORWARD=0", &p, want, sizeof(want)); }

    /* 9.20.10 LOCATION_FILTER, by how many fields are present. */
    memset(&p, 0, sizeof(p));
    p.has_location_filter = true;               /* StartGroup=0, StartObject=0: Next Object */
    p.location_filter.field_count = 2;
    { static const uint8_t want[] = { 0x01, 0x21, 0x02, 0x00, 0x00 };
      encode_params_expect("LOCATION_FILTER next object", &p, want, sizeof(want)); }

    memset(&p, 0, sizeof(p));
    p.has_location_filter = true;               /* StartGroup=1 only: the current group */
    p.location_filter.field_count = 1;
    p.location_filter.start_group = 1;
    { static const uint8_t want[] = { 0x01, 0x21, 0x01, 0x01 };
      encode_params_expect("LOCATION_FILTER current group", &p, want, sizeof(want)); }

    memset(&p, 0, sizeof(p));
    p.has_location_filter = true;               /* zero-length: no filter / remove */
    { static const uint8_t want[] = { 0x01, 0x21, 0x00 };
      encode_params_expect("LOCATION_FILTER removal", &p, want, sizeof(want)); }

    memset(&p, 0, sizeof(p));
    p.has_location_filter = true;               /* {3,0} .. group +2, object 7 */
    p.location_filter.field_count = 4;
    p.location_filter.start_group = 3; p.location_filter.start_object = 0;
    p.location_filter.end_group_delta = 2; p.location_filter.end_object = 7;
    { static const uint8_t want[] = { 0x01, 0x21, 0x04, 0x03, 0x00, 0x02, 0x07 };
      encode_params_expect("LOCATION_FILTER absolute range", &p, want, sizeof(want)); }

    memset(&p, 0, sizeof(p));
    p.has_location_filter = true;               /* 3 fields: end object omitted */
    p.location_filter.field_count = 3;
    p.location_filter.start_group = 1; p.location_filter.end_group_delta = 5;
    { static const uint8_t want[] = { 0x01, 0x21, 0x03, 0x01, 0x00, 0x05 };
      encode_params_expect("LOCATION_FILTER three fields", &p, want, sizeof(want)); }

    /* Ascending order with Type Deltas: 0x02 (+2), 0x10 (+14), 0x20 (+16),
     * 0x22 (+2), 0x35 (+19). */
    memset(&p, 0, sizeof(p));
    p.has_object_delivery_timeout = true; p.object_delivery_timeout_ms = 100;
    p.has_forward = true; p.forward = 1;
    p.has_subscriber_priority = true; p.subscriber_priority = 7;
    p.has_group_order = true; p.group_order = 2;
    p.has_include_properties = true; p.include_properties = 0;
    { static const uint8_t want[] = {
          0x05, 0x02, 0x64, 0x0E, 0x01, 0x10, 0x07, 0x02, 0x02, 0x13, 0x00 };
      encode_params_expect("ordered parameter block", &p, want, sizeof(want)); }

    /* 9.20.16 FILL_PARAMETERS: a length-prefixed block of parameters with no
     * count of its own (open ambiguity A1). Whole track: an empty nested filter. */
    memset(&p, 0, sizeof(p));
    p.has_fill = true;
    p.fill.has_location_filter = true;          /* field_count 0 */
    { static const uint8_t want[] = { 0x01, 0x23, 0x02, 0x21, 0x00 };
      encode_params_expect("FILL whole track", &p, want, sizeof(want)); }

    memset(&p, 0, sizeof(p));
    p.has_fill = true;
    p.fill.has_fill_timeout = true; p.fill.fill_timeout_ms = 0;
    p.fill.has_subscriber_priority = true; p.fill.subscriber_priority = 5;
    p.fill.has_location_filter = true;
    p.fill.location_filter.field_count = 1; p.fill.location_filter.start_group = 1;
    { static const uint8_t want[] = {
          0x01, 0x23, 0x07, 0x0A, 0x00, 0x16, 0x05, 0x01, 0x01, 0x01 };
      encode_params_expect("FILL with overrides", &p, want, sizeof(want)); }

    /* An empty parameter block is a bare zero count. */
    memset(&p, 0, sizeof(p));
    { static const uint8_t want[] = { 0x00 };
      encode_params_expect("no parameters", &p, want, sizeof(want)); }
}

static void t_params_encode_refusals(void)
{
    uint8_t buf[256];
    moq_buf_writer_t w;
    moq_d21_msg_params_t p;

    /* Every refusal leaves the writer exactly where it was. */
#define REFUSE(label, setup) do { \
        memset(&p, 0, sizeof(p)); setup; \
        moq_buf_writer_init(&w, buf, sizeof(buf)); \
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_msg_params(&w, &p), \
                              (int)MOQ_ERR_INVAL); \
        if (moq_buf_writer_offset(&w) != 0) { \
            fprintf(stderr, "FAIL: %s left bytes behind\n", label); failures++; } \
    } while (0)
    REFUSE("FORWARD=2",       (p.has_forward = true, p.forward = 2));
    REFUSE("GROUP_ORDER=0",   (p.has_group_order = true, p.group_order = 0));
    REFUSE("GROUP_ORDER=3",   (p.has_group_order = true, p.group_order = 3));
    REFUSE("INCLUDE_PROPERTIES=2",
           (p.has_include_properties = true, p.include_properties = 2));
    REFUSE("filter with 5 fields",
           (p.has_location_filter = true, p.location_filter.field_count = 5));
    REFUSE("filter end overflows 2^64-1",
           (p.has_location_filter = true, p.location_filter.field_count = 3,
            p.location_filter.start_group = UINT64_MAX,
            p.location_filter.end_group_delta = 1));
    REFUSE("range filters are never emitted", (p.range_filter_params = 1));
    REFUSE("track namespace prefix is never emitted",
           (p.has_track_namespace_prefix = true));
    REFUSE("fill with a range filter",
           (p.has_fill = true, p.fill.range_filter_params = 1));
    REFUSE("fill with GROUP_ORDER=9",
           (p.has_fill = true, p.fill.has_group_order = true,
            p.fill.group_order = 9));
    REFUSE("too many auth tokens",
           (p.auth_token_count = MOQ_D21_MAX_AUTH_TOKENS + 1));
#undef REFUSE
}

static void t_params_decode_basic(void)
{
    moq_d21_msg_params_t o;

    /* Round trip of the ordered block. */
    {
        static const uint8_t b[] = { 0x05, 0x02, 0x64, 0x0E, 0x01, 0x10, 0x07,
                                     0x02, 0x02, 0x13, 0x00 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(b, sizeof(b), ALL_BITS, &o),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(o.has_object_delivery_timeout);
        MOQ_TEST_CHECK_EQ_U64(o.object_delivery_timeout_ms, 100);
        MOQ_TEST_CHECK(o.has_forward && o.forward == 1);
        MOQ_TEST_CHECK(o.has_subscriber_priority && o.subscriber_priority == 7);
        MOQ_TEST_CHECK(o.has_group_order && o.group_order == 2);
        MOQ_TEST_CHECK(o.has_include_properties && o.include_properties == 0);
    }

    /* Each Location Filter shape decodes to the same field_count it was sent with. */
    {
        static const uint8_t none[]  = { 0x01, 0x21, 0x00 };
        static const uint8_t one[]   = { 0x01, 0x21, 0x01, 0x01 };
        static const uint8_t four[]  = { 0x01, 0x21, 0x04, 0x03, 0x00, 0x02, 0x07 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(none, sizeof(none), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK(o.has_location_filter && o.location_filter.field_count == 0);
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(one, sizeof(one), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK(o.location_filter.field_count == 1 && o.location_filter.start_group == 1);
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(four, sizeof(four), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK(o.location_filter.field_count == 4);
        MOQ_TEST_CHECK_EQ_U64(o.location_filter.start_group, 3);
        MOQ_TEST_CHECK_EQ_U64(o.location_filter.start_object, 0);
        MOQ_TEST_CHECK_EQ_U64(o.location_filter.end_group_delta, 2);
        MOQ_TEST_CHECK_EQ_U64(o.location_filter.end_object, 7);
    }

    /* LARGEST_OBJECT is two consecutive varints (9.20 "Location"). */
    {
        static const uint8_t b[] = { 0x01, 0x09, 0x07, 0x09 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(b, sizeof(b), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK(o.has_largest && o.largest_group == 7 && o.largest_object == 9);
    }

    /* Non-minimal vi64 encodings are valid (8.1): 100 as a two-byte vi64. */
    {
        static const uint8_t b[] = { 0x01, 0x02, 0x80, 0x64 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(b, sizeof(b), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(o.object_delivery_timeout_ms, 100);
    }

    /* AUTHORIZATION_TOKEN may repeat with a zero delta (8.9). */
    {
        static const uint8_t b[] = {
            0x02, 0x03, 0x02, 0x03, 0x00,        /* USE_VALUE, token type 0, empty */
            0x00, 0x02, 0x02, 0x07 };            /* delta 0: USE_ALIAS 7 */
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(b, sizeof(b), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(o.auth_token_count, 2);
        MOQ_TEST_CHECK_EQ_INT((int)o.auth_tokens[0].alias_type, 3);
        MOQ_TEST_CHECK_EQ_INT((int)o.auth_tokens[1].alias_type, 2);
        MOQ_TEST_CHECK_EQ_U64(o.auth_tokens[1].alias, 7);
    }
}

static void t_params_decode_violations(void)
{
    moq_d21_msg_params_t o;
#define PROTO(label, mask, ...) do { \
        static const uint8_t b_[] = { __VA_ARGS__ }; \
        moq_result_t rc_ = decode_block(b_, sizeof(b_), (mask), &o); \
        if (rc_ != MOQ_ERR_PROTO) { \
            fprintf(stderr, "FAIL: %s: got %d, want PROTO\n", label, (int)rc_); \
            failures++; } \
    } while (0)

    /* Repeats and ordering (9.20): only AUTHORIZATION_TOKEN and Range Filters
     * may repeat; a zero delta on anything else is a violation. */
    PROTO("duplicate FORWARD", ALL_BITS, 0x02, 0x10, 0x01, 0x00, 0x01);
    PROTO("zero delta as the first parameter", ALL_BITS, 0x01, 0x00, 0x00);
    PROTO("duplicate LOCATION_FILTER", ALL_BITS,
          0x02, 0x21, 0x00, 0x00, 0x00);
    /* A repeat of a token when the message does not permit tokens at all. */
    PROTO("repeat token not permitted", MOQ_D21_MASK_SUBSCRIBE_OK,
          0x02, 0x03, 0x02, 0x03, 0x00, 0x00, 0x02, 0x03, 0x00);

    /* An unknown parameter cannot be skipped (9.20): 0x24 and 0x30 are
     * unassigned here, and 0x9D is a GREASE value. */
    PROTO("unknown 0x24", ALL_BITS, 0x01, 0x24, 0x00);
    PROTO("unknown 0x30", ALL_BITS, 0x01, 0x30, 0x00);
    PROTO("GREASE 0x9D", ALL_BITS, 0x01, 0x80 | 0x00, 0x9D, 0x00);

    /* Enumerated values out of range (9.20.9, 9.20.19, 9.20.22). */
    PROTO("FORWARD=2", ALL_BITS, 0x01, 0x10, 0x02);
    PROTO("FORWARD=255", ALL_BITS, 0x01, 0x10, 0xFF);
    PROTO("GROUP_ORDER=0", ALL_BITS, 0x01, 0x22, 0x00);
    PROTO("GROUP_ORDER=3", ALL_BITS, 0x01, 0x22, 0x03);
    PROTO("INCLUDE_PROPERTIES=2", ALL_BITS, 0x01, 0x35, 0x02);

    /* LOCATION_FILTER structure (9.20.10). A fifth field, a length that does not
     * cover whole vi64 fields, and an overflowing end group all close. */
    PROTO("filter with 5 fields", ALL_BITS,
          0x01, 0x21, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00);
    PROTO("filter ends mid-vi64", ALL_BITS, 0x01, 0x21, 0x01, 0x80);
    {
        /* StartGroup = 2^64-1, EndGroupDelta = 1: 2^64 does not fit. */
        PROTO("filter end group overflows", ALL_BITS,
              0x01, 0x21, 0x0B, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
              0x00, 0x01);
    }

    /* Nested FILL_PARAMETERS (9.20.16): only its whitelist, nothing nested, and a
     * truncated nested value is a protocol violation, not a "need more bytes". */
    PROTO("fill containing an auth token", ALL_BITS,
          0x01, 0x23, 0x05, 0x03, 0x02, 0x03, 0x00, 0x00);
    PROTO("fill containing a fill", ALL_BITS, 0x01, 0x23, 0x02, 0x23, 0x00);
    PROTO("fill containing FORWARD", ALL_BITS, 0x01, 0x23, 0x02, 0x10, 0x01);
    PROTO("fill containing a track property filter", ALL_BITS,
          0x01, 0x23, 0x02, 0x29, 0x00);
    PROTO("fill with unknown nested type", ALL_BITS, 0x01, 0x23, 0x02, 0x30, 0x00);
    PROTO("fill nested value truncated", ALL_BITS, 0x01, 0x23, 0x01, 0x20);
    PROTO("fill with invalid nested GROUP_ORDER", ALL_BITS,
          0x01, 0x23, 0x02, 0x22, 0x07);
    PROTO("fill with duplicate nested parameter", ALL_BITS,
          0x01, 0x23, 0x04, 0x20, 0x01, 0x00, 0x02);
    PROTO("fill with overflowing nested filter", ALL_BITS,
          0x01, 0x23, 0x0D, 0x21, 0x0B, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
          0xFF, 0xFF, 0x00, 0x01);

    /* Range Filter structure: SetID must be present once Length > 0 is
     * consumed, and the content must be whole vi64 values. */
    PROTO("range filter value truncated", ALL_BITS, 0x01, 0x26, 0x02, 0x00, 0x80);
    PROTO("object property filter missing property type", ALL_BITS,
          0x01, 0x28, 0x01, 0x00);
#undef PROTO

    /* A value cut off by the end of the message is not a protocol violation of
     * the value; it is the end of the buffer. */
    {
        static const uint8_t b[] = { 0x01, 0x10 };    /* FORWARD with no value */
        MOQ_TEST_CHECK(decode_block(b, sizeof(b), ALL_BITS, &o) < 0);
    }
    {
        static const uint8_t b[] = { 0x02, 0x10, 0x01 };   /* count 2, one present */
        MOQ_TEST_CHECK(decode_block(b, sizeof(b), ALL_BITS, &o) < 0);
    }
}

static void t_params_fill_decode(void)
{
    moq_d21_msg_params_t o;

    /* The whole-track fill: FILL_PARAMETERS carrying an empty LOCATION_FILTER. */
    {
        static const uint8_t b[] = { 0x01, 0x23, 0x02, 0x21, 0x00 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(b, sizeof(b), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK(o.has_fill);
        MOQ_TEST_CHECK(o.fill.has_location_filter);
        MOQ_TEST_CHECK_EQ_INT((int)o.fill.location_filter.field_count, 0);
        /* FILL_PARAMETERS is its own scope: the message itself has no filter. */
        MOQ_TEST_CHECK(!o.has_location_filter && !o.has_subscriber_priority);
    }

    /* A fill with overrides, and the same Parameter Type in both scopes: the
     * message carries SUBSCRIBER_PRIORITY 9 and the fill overrides it with 5
     * (9.20.16: "a Parameter Type MAY appear both in the message and inside"). */
    {
        static const uint8_t b[] = {
            0x02,
            0x20, 0x09,                                   /* message priority */
            0x03, 0x07, 0x0A, 0x00, 0x16, 0x05, 0x01, 0x01, 0x01 };
        /* delta 3 from 0x20 -> 0x23 FILL_PARAMETERS, length 7 */
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(b, sizeof(b), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK(o.has_subscriber_priority && o.subscriber_priority == 9);
        MOQ_TEST_CHECK(o.has_fill);
        MOQ_TEST_CHECK(o.fill.has_fill_timeout && o.fill.fill_timeout_ms == 0);
        MOQ_TEST_CHECK(o.fill.has_subscriber_priority && o.fill.subscriber_priority == 5);
        MOQ_TEST_CHECK(o.fill.has_location_filter &&
                       o.fill.location_filter.field_count == 1 &&
                       o.fill.location_filter.start_group == 1);
        MOQ_TEST_CHECK(!o.fill.has_group_order);
    }

    /* A fill may carry Range Filters; they are counted, not acted on. */
    {
        static const uint8_t b[] = { 0x01, 0x23, 0x06, 0x26, 0x04, 0x00, 0x03, 0x02, 0x05 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(b, sizeof(b), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)o.fill.range_filter_params, 1);
        MOQ_TEST_CHECK_EQ_U64(o.fill.range_filter_ranges, 2);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)o.range_filter_params, 0);
    }

    /* An empty FILL_PARAMETERS is still a request for a fill (9.20.16: its
     * presence is what requests one). */
    {
        static const uint8_t b[] = { 0x01, 0x23, 0x00 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(b, sizeof(b), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK(o.has_fill);
    }
}

static void t_params_range_filters(void)
{
    moq_d21_msg_params_t o;

    /* 8.6: ranges 3-5 and 10-15 encode as Start=3, End=2, Start=5, End=5. */
    {
        static const uint8_t b[] = { 0x01, 0x26, 0x05, 0x00, 0x03, 0x02, 0x05, 0x05 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(b, sizeof(b), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)o.range_filter_params, 1);
        MOQ_TEST_CHECK_EQ_U64(o.range_filter_ranges, 2);
        MOQ_TEST_CHECK(!o.range_filter_invalid);
    }
    /* The last End is optional: Start only is one open-ended Range. */
    {
        static const uint8_t b[] = { 0x01, 0x26, 0x02, 0x00, 0x05 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(b, sizeof(b), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(o.range_filter_ranges, 1);
    }
    /* Length 0 means no filter: present, no Ranges (8.6). */
    {
        static const uint8_t b[] = { 0x01, 0x26, 0x00 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(b, sizeof(b), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)o.range_filter_params, 1);
        MOQ_TEST_CHECK_EQ_U64(o.range_filter_ranges, 0);
        MOQ_TEST_CHECK(!o.range_filter_invalid);
    }
    /* Filters repeat with a zero delta (3.3.2); both are counted. */
    {
        static const uint8_t b[] = { 0x02, 0x26, 0x02, 0x00, 0x05, 0x00, 0x02, 0x01, 0x07 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(b, sizeof(b), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)o.range_filter_params, 2);
        MOQ_TEST_CHECK_EQ_U64(o.range_filter_ranges, 2);
    }
    /* A repeat across different filter types is two different parameters. */
    {
        static const uint8_t b[] = { 0x02, 0x25, 0x02, 0x00, 0x01, 0x01, 0x02, 0x00, 0x02 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(b, sizeof(b), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)o.range_filter_params, 2);
    }

    /* The values the draft answers with INVALID_FILTER are flagged, and the decode
     * still succeeds: they are request errors, not session errors. */
    {
        /* Priority Filter value above 255 (9.20.13). */
        static const uint8_t prio[] = { 0x01, 0x27, 0x04, 0x00, 0x81, 0x2C, 0x00 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(prio, sizeof(prio), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK(o.range_filter_invalid);
        /* 255 is fine. */
        static const uint8_t ok255[] = { 0x01, 0x27, 0x03, 0x00, 0x80, 0xFF };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(ok255, sizeof(ok255), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK(!o.range_filter_invalid);
        /* The boundary: 256 is invalid, and the limit applies to the running
         * absolute value, not each delta (200 then +100 reaches 300). */
        static const uint8_t p256[] = { 0x01, 0x27, 0x04, 0x00, 0x81, 0x00, 0x00 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(p256, sizeof(p256), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK(o.range_filter_invalid);
        static const uint8_t p_sum[] = { 0x01, 0x27, 0x04, 0x00, 0x80, 0xC8, 0x64 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(p_sum, sizeof(p_sum), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK(o.range_filter_invalid);
        /* The 255 limit is for the Priority Filter only. */
        static const uint8_t big_id[] = { 0x01, 0x26, 0x04, 0x00, 0x81, 0x2C, 0x00 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(big_id, sizeof(big_id), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK(!o.range_filter_invalid);
        /* An odd Property Type in the Object/Track Property filter (9.20.14/15). */
        static const uint8_t odd_obj[] = { 0x01, 0x28, 0x04, 0x00, 0x03, 0x01, 0x00 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(odd_obj, sizeof(odd_obj), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK(o.range_filter_invalid);
        static const uint8_t odd_trk[] = { 0x01, 0x29, 0x04, 0x00, 0x03, 0x01, 0x00 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(odd_trk, sizeof(odd_trk), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK(o.range_filter_invalid);
        static const uint8_t even_obj[] = { 0x01, 0x28, 0x04, 0x00, 0x04, 0x01, 0x00 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(even_obj, sizeof(even_obj), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK(!o.range_filter_invalid);
        /* A Start delta of 2^64-1 followed by an End delta of 1 overflows (8.6). */
        static const uint8_t ovf[] = { 0x01, 0x26, 0x0B, 0x00,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01 };
        MOQ_TEST_CHECK_EQ_INT((int)decode_block(ovf, sizeof(ovf), ALL_BITS, &o), (int)MOQ_OK);
        MOQ_TEST_CHECK(o.range_filter_invalid);
    }
}

/* The per-message legality matrix. The expected sets are written out here from
 * each parameter's own "MAY appear in" text (wire reference, section 9.20), not
 * derived from the header's masks, so a wrong mask cannot pass by agreeing with
 * itself. For every message and every defined parameter type, a minimal valid
 * value must decode when the type is allowed and be PROTOCOL_VIOLATION when not. */
typedef struct {
    const char *name;
    uint32_t    mask;
    const uint8_t *allowed;     /* parameter types, terminated by 0xFF */
} matrix_row_t;

static void t_params_legality_matrix(void)
{
    /* Parameter types and a one-parameter block that is valid for each. The
     * block is `count, delta (== type), value`. */
    typedef struct { uint8_t type; uint8_t value[4]; size_t vlen; } pt_t;
    static const pt_t TYPES[] = {
        { 0x02, { 0x00 }, 1 }, { 0x03, { 0x02, 0x03, 0x00 }, 3 },
        { 0x04, { 0x00 }, 1 }, { 0x06, { 0x00 }, 1 }, { 0x08, { 0x00 }, 1 },
        { 0x09, { 0x00, 0x00 }, 2 }, { 0x0A, { 0x00 }, 1 }, { 0x10, { 0x00 }, 1 },
        { 0x20, { 0x00 }, 1 }, { 0x21, { 0x00 }, 1 }, { 0x22, { 0x01 }, 1 },
        { 0x23, { 0x00 }, 1 }, { 0x25, { 0x00 }, 1 }, { 0x26, { 0x00 }, 1 },
        { 0x27, { 0x00 }, 1 }, { 0x28, { 0x00 }, 1 }, { 0x29, { 0x00 }, 1 },
        { 0x32, { 0x00 }, 1 }, { 0x34, { 0x00 }, 1 }, { 0x35, { 0x01 }, 1 },
    };
#define E 0xFF
    static const uint8_t SUBSCRIBE[]   = { 0x02,0x03,0x04,0x06,0x10,0x20,0x21,0x22,0x23,0x25,0x26,0x27,0x28,0x32,0x35,E };
    static const uint8_t SUB_TRACKS[]  = { 0x02,0x03,0x04,0x06,0x10,0x20,0x21,0x22,0x23,0x25,0x26,0x27,0x28,0x29,0x32,0x35,E };
    static const uint8_t UPDATE[]      = { 0x02,0x03,0x06,0x10,0x20,0x21,0x23,0x25,0x26,0x27,0x28,0x29,0x32,0x34,E };
    static const uint8_t PUBLISH[]     = { 0x02,0x03,0x06,0x08,0x09,0x10,0x20,0x21,0x22,E };
    static const uint8_t FETCH[]       = { 0x03,0x0A,0x20,0x21,0x22,0x25,0x26,0x27,0x28,0x35,E };
    static const uint8_t TRACK_STATUS[]= { 0x03,0x35,E };
    static const uint8_t NS_REQUEST[]  = { 0x03,E };
    static const uint8_t SUB_OK[]      = { 0x08,0x09,E };
    static const uint8_t NONE[]        = { E };
    static const uint8_t NOTIFY[]      = { 0x09,0x10,0x21,E };
    static const uint8_t EXPIRES_ONLY[]= { 0x08,E };
    static const uint8_t FILL_NESTED[] = { 0x0A,0x20,0x21,0x22,0x25,0x26,0x27,0x28,E };
#undef E
    const matrix_row_t rows[] = {
        { "SUBSCRIBE", MOQ_D21_MASK_SUBSCRIBE, SUBSCRIBE },
        { "SUBSCRIBE_TRACKS", MOQ_D21_MASK_SUBSCRIBE_TRACKS, SUB_TRACKS },
        { "REQUEST_UPDATE", MOQ_D21_MASK_REQUEST_UPDATE, UPDATE },
        { "PUBLISH", MOQ_D21_MASK_PUBLISH, PUBLISH },
        { "FETCH", MOQ_D21_MASK_FETCH, FETCH },
        { "TRACK_STATUS", MOQ_D21_MASK_TRACK_STATUS, TRACK_STATUS },
        { "PUBLISH_NAMESPACE/SUBSCRIBE_NAMESPACE", MOQ_D21_MASK_NAMESPACE_REQUEST, NS_REQUEST },
        { "SUBSCRIBE_OK", MOQ_D21_MASK_SUBSCRIBE_OK, SUB_OK },
        { "FETCH_OK", MOQ_D21_MASK_FETCH_OK, NONE },
        { "PUBLISH_STATE_NOTIFY", MOQ_D21_MASK_PUBLISH_STATE_NOTIFY, NOTIFY },
        { "PUBLISH_OK", MOQ_D21_MASK_PUBLISH_OK, EXPIRES_ONLY },
        { "REQUEST_UPDATE_OK", MOQ_D21_MASK_REQUEST_UPDATE_OK, SUB_OK },
        { "TRACK_STATUS_OK", MOQ_D21_MASK_TRACK_STATUS_OK, SUB_OK },
        { "NAMESPACE_OK", MOQ_D21_MASK_NAMESPACE_OK, EXPIRES_ONLY },
        { "FILL_PARAMETERS contents", MOQ_D21_MASK_FILL_NESTED, FILL_NESTED },
    };
    for (size_t m = 0; m < sizeof(rows) / sizeof(rows[0]); m++) {
        for (size_t t = 0; t < sizeof(TYPES) / sizeof(TYPES[0]); t++) {
            bool expect_ok = false;
            for (const uint8_t *a = rows[m].allowed; *a != 0xFF; a++)
                if (*a == TYPES[t].type) expect_ok = true;
            uint8_t b[8];
            size_t n = 0;
            b[n++] = 0x01;                        /* one parameter */
            b[n++] = TYPES[t].type;               /* delta == type as the first */
            memcpy(b + n, TYPES[t].value, TYPES[t].vlen);
            n += TYPES[t].vlen;
            moq_d21_msg_params_t o;
            moq_result_t rc = decode_block(b, n, rows[m].mask, &o);
            moq_result_t want = expect_ok ? MOQ_OK : MOQ_ERR_PROTO;
            if (rc != want) {
                fprintf(stderr, "FAIL: %s with parameter 0x%02X: got %d, want %d\n",
                        rows[m].name, TYPES[t].type, (int)rc, (int)want);
                failures++;
            }
        }
    }
}

static void t_params_roundtrip(void)
{
    /* Every encodable field, through encode and decode, under a mask that allows
     * them all. The count the encoder writes must match what it emitted. */
    moq_d21_msg_params_t p;
    memset(&p, 0, sizeof(p));
    p.has_object_delivery_timeout = true; p.object_delivery_timeout_ms = 1500;
    p.auth_token_count = 2;
    p.auth_tokens[0].alias_type = 1;                 /* REGISTER */
    p.auth_tokens[0].alias = 4;
    p.auth_tokens[0].token_type = 0;
    p.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("tok");
    p.auth_tokens[1].alias_type = 2;                 /* USE_ALIAS */
    p.auth_tokens[1].alias = 4;
    p.has_rendezvous_timeout = true; p.rendezvous_timeout_ms = 250;
    p.has_subgroup_delivery_timeout = true; p.subgroup_delivery_timeout_ms = 900;
    p.has_expires = true; p.expires_ms = 60000;
    p.has_largest = true; p.largest_group = 12; p.largest_object = 3;
    p.has_fill_timeout = true; p.fill_timeout_ms = 40;
    p.has_forward = true; p.forward = 1;
    p.has_subscriber_priority = true; p.subscriber_priority = 128;
    p.has_location_filter = true;
    p.location_filter.field_count = 4;
    p.location_filter.start_group = 9; p.location_filter.start_object = 1;
    p.location_filter.end_group_delta = 3; p.location_filter.end_object = 2;
    p.has_group_order = true; p.group_order = 2;
    p.has_fill = true;
    p.fill.has_fill_timeout = true; p.fill.fill_timeout_ms = 1000;
    p.fill.has_subscriber_priority = true; p.fill.subscriber_priority = 1;
    p.fill.has_group_order = true; p.fill.group_order = 1;
    p.fill.has_location_filter = true;
    p.fill.location_filter.field_count = 2;
    p.fill.location_filter.start_group = 2; p.fill.location_filter.start_object = 0;
    p.has_new_group_request = true; p.new_group_request = 0;
    p.has_include_properties = true; p.include_properties = 0;

    uint8_t buf[512];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_msg_params(&w, &p), (int)MOQ_OK);

    moq_d21_msg_params_t o;
    MOQ_TEST_CHECK_EQ_INT(
        (int)decode_block(buf, moq_buf_writer_offset(&w), ALL_BITS, &o),
        (int)MOQ_OK);
    MOQ_TEST_CHECK(o.has_object_delivery_timeout && o.object_delivery_timeout_ms == 1500);
    MOQ_TEST_CHECK_EQ_SIZE(o.auth_token_count, 2);
    MOQ_TEST_CHECK(o.auth_tokens[0].alias_type == 1 && o.auth_tokens[0].alias == 4);
    MOQ_TEST_CHECK(o.auth_tokens[0].token_value.len == 3 &&
                   memcmp(o.auth_tokens[0].token_value.data, "tok", 3) == 0);
    MOQ_TEST_CHECK(o.auth_tokens[1].alias_type == 2 && o.auth_tokens[1].alias == 4);
    MOQ_TEST_CHECK(o.has_rendezvous_timeout && o.rendezvous_timeout_ms == 250);
    MOQ_TEST_CHECK(o.has_subgroup_delivery_timeout && o.subgroup_delivery_timeout_ms == 900);
    MOQ_TEST_CHECK(o.has_expires && o.expires_ms == 60000);
    MOQ_TEST_CHECK(o.has_largest && o.largest_group == 12 && o.largest_object == 3);
    MOQ_TEST_CHECK(o.has_fill_timeout && o.fill_timeout_ms == 40);
    MOQ_TEST_CHECK(o.has_forward && o.forward == 1);
    MOQ_TEST_CHECK(o.has_subscriber_priority && o.subscriber_priority == 128);
    MOQ_TEST_CHECK(o.has_location_filter && o.location_filter.field_count == 4 &&
                   o.location_filter.start_group == 9 && o.location_filter.end_object == 2);
    MOQ_TEST_CHECK(o.has_group_order && o.group_order == 2);
    MOQ_TEST_CHECK(o.has_fill && o.fill.fill_timeout_ms == 1000 &&
                   o.fill.subscriber_priority == 1 && o.fill.group_order == 1 &&
                   o.fill.location_filter.field_count == 2 &&
                   o.fill.location_filter.start_group == 2);
    MOQ_TEST_CHECK(o.has_new_group_request && o.new_group_request == 0);
    MOQ_TEST_CHECK(o.has_include_properties && o.include_properties == 0);
}

int main(void)
{
    t_setup_options_encode();
    t_setup_options_decode();
    t_params_encode_vectors();
    t_params_encode_refusals();
    t_params_decode_basic();
    t_params_decode_violations();
    t_params_fill_decode();
    t_params_range_filters();
    t_params_legality_matrix();
    t_params_roundtrip();

    if (failures)
        fprintf(stderr, "test_control_d21: %d byte-vector failures\n", failures);
    MOQ_TEST_PASS("test_control_d21");
}
