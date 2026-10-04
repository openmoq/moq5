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
static const moq_bytes_t NO_BYTES = { NULL, 0 };

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


/* == 4c. SUBSCRIBE family, REQUEST_OK, REQUEST_ERROR ======================= */

/* Frame check: Type (vi64), Length (16), then the body, with the length field
 * agreeing with the bytes that follow it. */
static void check_message(const char *what, const uint8_t *got, size_t got_len,
                          const uint8_t *want, size_t want_len)
{
    check_bytes(what, got, got_len, want, want_len);
}

static moq_control_envelope_t decode_env(const uint8_t *b, size_t n, uint64_t want_type)
{
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, b, n);
    moq_control_envelope_t env;
    memset(&env, 0, sizeof(env));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_envelope(&r, &env), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(env.msg_type, want_type);
    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), 0);
    return env;
}

static void t_subscribe(void)
{
    /* 9.6: Request ID 42, namespace live/sports, name video, no parameters. */
    uint8_t buf[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live"), MOQ_BYTES_LITERAL("sports") };
    moq_namespace_t ns = { parts, 2 };
    moq_d21_msg_params_t none;
    memset(&none, 0, sizeof(none));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_subscribe(&w, 42, &ns,
        MOQ_BYTES_LITERAL("video"), &none), (int)MOQ_OK);
    static const uint8_t want[] = {
        0x03, 0x00, 0x15, 0x2A, 0x02,
        0x04, 'l', 'i', 'v', 'e', 0x06, 's', 'p', 'o', 'r', 't', 's',
        0x05, 'v', 'i', 'd', 'e', 'o', 0x00 };
    check_message("SUBSCRIBE", buf, moq_buf_writer_offset(&w), want, sizeof(want));

    moq_control_envelope_t env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_SUBSCRIBE);
    moq_bytes_t dparts[32];
    moq_d21_subscribe_t sub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_subscribe(env.payload, env.payload_len,
                                                        dparts, 32, &sub), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(sub.request_id, 42);
    MOQ_TEST_CHECK_EQ_SIZE(sub.track_namespace.count, 2);
    MOQ_TEST_CHECK(sub.track_name.len == 5 && memcmp(sub.track_name.data, "video", 5) == 0);
}

static void t_subscribe_params(void)
{
    /* FORWARD=1 then the Next Object Location Filter: 0x10, then +0x11 -> 0x21. */
    uint8_t buf[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("a") };
    moq_namespace_t ns = { parts, 1 };
    moq_d21_msg_params_t p;
    memset(&p, 0, sizeof(p));
    p.has_forward = true; p.forward = 1;
    p.has_location_filter = true; p.location_filter.field_count = 2;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_subscribe(&w, 2, &ns,
        MOQ_BYTES_LITERAL("t"), &p), (int)MOQ_OK);
    static const uint8_t want[] = {
        0x03, 0x00, 0x0D, 0x02, 0x01, 0x01, 'a', 0x01, 't',
        0x02, 0x10, 0x01, 0x11, 0x02, 0x00, 0x00 };
    check_message("SUBSCRIBE with filter", buf, moq_buf_writer_offset(&w), want, sizeof(want));

    moq_control_envelope_t env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_SUBSCRIBE);
    moq_bytes_t dparts[32];
    moq_d21_subscribe_t sub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_subscribe(env.payload, env.payload_len,
                                                        dparts, 32, &sub), (int)MOQ_OK);
    MOQ_TEST_CHECK(sub.params.has_forward && sub.params.forward == 1);
    MOQ_TEST_CHECK(sub.params.has_location_filter &&
                   sub.params.location_filter.field_count == 2);

    /* SUBSCRIBE does not take EXPIRES or LARGEST_OBJECT (response parameters). */
    memset(&p, 0, sizeof(p));
    p.has_expires = true;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_subscribe(&w, 2, &ns,
        MOQ_BYTES_LITERAL("t"), &p), (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);

    /* ... and the decoder refuses what the encoder would: EXPIRES in a SUBSCRIBE
     * body is a PROTOCOL_VIOLATION. */
    static const uint8_t bad[] = { 0x02, 0x01, 0x01, 'a', 0x01, 't', 0x01, 0x08, 0x00 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_subscribe(bad, sizeof(bad),
                                                        dparts, 32, &sub), (int)MOQ_ERR_PROTO);
}

static void t_namespace_limits(void)
{
    /* 8.7: a Track Namespace Field must not be empty, and there are at most 32
     * fields; both close the session. */
    moq_bytes_t dparts[40];
    moq_d21_subscribe_t sub;
    static const uint8_t empty_field[] = { 0x02, 0x01, 0x00, 0x01, 't', 0x00 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_subscribe(empty_field, sizeof(empty_field),
                                                        dparts, 40, &sub), (int)MOQ_ERR_PROTO);
    uint8_t many[160];
    size_t n = 0;
    many[n++] = 0x02;                       /* request id */
    many[n++] = 33;                         /* 33 fields */
    for (int i = 0; i < 33; i++) { many[n++] = 0x01; many[n++] = 'x'; }
    many[n++] = 0x01; many[n++] = 't'; many[n++] = 0x00;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_subscribe(many, n, dparts, 40, &sub),
                          (int)MOQ_ERR_PROTO);
    /* Exactly 32 is fine. */
    n = 0;
    many[n++] = 0x02;
    many[n++] = 32;
    for (int i = 0; i < 32; i++) { many[n++] = 0x01; many[n++] = 'x'; }
    many[n++] = 0x01; many[n++] = 't'; many[n++] = 0x00;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_subscribe(many, n, dparts, 40, &sub),
                          (int)MOQ_OK);
    /* Zero fields is a valid (root) namespace (8.7: between 0 and 32). */
    static const uint8_t root[] = { 0x02, 0x00, 0x01, 't', 0x00 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_subscribe(root, sizeof(root),
                                                        dparts, 40, &sub), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(sub.track_namespace.count, 0);
}

static void t_subscribe_ok(void)
{
    /* 9.7: Track Alias 5, LARGEST_OBJECT {3,1}, then a Track Property
     * (DEFAULT_PUBLISHER_PRIORITY 0x0E = 5) that fills the rest of the message. */
    uint8_t buf[64];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_d21_msg_params_t p;
    memset(&p, 0, sizeof(p));
    p.has_largest = true; p.largest_group = 3; p.largest_object = 1;
    static const uint8_t props[] = { 0x0E, 0x05 };
    moq_bytes_t tp = { props, sizeof(props) };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_subscribe_ok(&w, 5, &p, tp), (int)MOQ_OK);
    static const uint8_t want[] = { 0x04, 0x00, 0x07, 0x05, 0x01, 0x09, 0x03, 0x01, 0x0E, 0x05 };
    check_message("SUBSCRIBE_OK", buf, moq_buf_writer_offset(&w), want, sizeof(want));

    moq_control_envelope_t env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_SUBSCRIBE_OK);
    moq_d21_subscribe_ok_t ok;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_subscribe_ok(env.payload, env.payload_len, &ok),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(ok.track_alias, 5);
    MOQ_TEST_CHECK(ok.params.has_largest && ok.params.largest_group == 3 &&
                   ok.params.largest_object == 1);
    MOQ_TEST_CHECK_EQ_SIZE(ok.track_properties.len, 2);
    MOQ_TEST_CHECK(!ok.dynamic_groups && !ok.track_properties_unsupported);

    /* DYNAMIC_GROUPS = 1 (10.6) is surfaced; a value above 1 closes. */
    {
        static const uint8_t dg[] = { 0x05, 0x00, 0x30, 0x01 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_subscribe_ok(dg, sizeof(dg), &ok), (int)MOQ_OK);
        MOQ_TEST_CHECK(ok.dynamic_groups);
        static const uint8_t dg2[] = { 0x05, 0x00, 0x30, 0x02 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_subscribe_ok(dg2, sizeof(dg2), &ok),
                              (int)MOQ_ERR_PROTO);
    }
    /* A Mandatory Track Property (0x4000..0x7FFF, 3.6) is surfaced as
     * unsupported rather than rejected, so the session can cancel the
     * subscription (UNSUPPORTED_EXTENSION). 0x4000 is C0 40 00 as a vi64. */
    {
        static const uint8_t mp[] = { 0x05, 0x00, 0xC0, 0x40, 0x00, 0x00 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_subscribe_ok(mp, sizeof(mp), &ok), (int)MOQ_OK);
        MOQ_TEST_CHECK(ok.track_properties_unsupported);
    }
    /* SUBSCRIBE_OK carries only EXPIRES and LARGEST_OBJECT (9.20). */
    {
        static const uint8_t fwd[] = { 0x05, 0x01, 0x10, 0x00 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_subscribe_ok(fwd, sizeof(fwd), &ok),
                              (int)MOQ_ERR_PROTO);
    }
}

static void t_request_update(void)
{
    /* 9.5: Request ID 6, FORWARD 0. */
    uint8_t buf[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_d21_msg_params_t p;
    memset(&p, 0, sizeof(p));
    p.has_forward = true; p.forward = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_request_update(&w, 6, &p), (int)MOQ_OK);
    static const uint8_t want[] = { 0x02, 0x00, 0x04, 0x06, 0x01, 0x10, 0x00 };
    check_message("REQUEST_UPDATE", buf, moq_buf_writer_offset(&w), want, sizeof(want));

    moq_control_envelope_t env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_REQUEST_UPDATE);
    moq_d21_request_update_t u;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_request_update(env.payload, env.payload_len, &u),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(u.request_id, 6);
    MOQ_TEST_CHECK(u.params.has_forward && u.params.forward == 0);

    /* GROUP_ORDER may not be updated (9.20.9 lists SUBSCRIBE, PUBLISH,
     * SUBSCRIBE_TRACKS and FETCH only); on the wire it is a violation. */
    static const uint8_t go[] = { 0x06, 0x01, 0x22, 0x01 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_request_update(go, sizeof(go), &u),
                          (int)MOQ_ERR_PROTO);
    memset(&p, 0, sizeof(p));
    p.has_group_order = true; p.group_order = 1;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_request_update(&w, 6, &p), (int)MOQ_ERR_INVAL);

    /* A fill is requested by an update that carries FILL_PARAMETERS (9.5.1). */
    memset(&p, 0, sizeof(p));
    p.has_location_filter = true; p.location_filter.field_count = 1; p.location_filter.start_group = 1;
    p.has_fill = true; p.fill.has_location_filter = true;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_request_update(&w, 8, &p), (int)MOQ_OK);
    env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_REQUEST_UPDATE);
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_request_update(env.payload, env.payload_len, &u),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(u.params.has_fill && u.params.has_location_filter);
}

static void t_request_ok(void)
{
    /* 9.3: PUBLISH_OK is a REQUEST_OK; with no parameters and no properties the
     * body is a bare zero count. */
    uint8_t buf[64];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_request_ok(
        &w, MOQ_D21_REQUEST_OK_PUBLISH, NULL, NO_BYTES), (int)MOQ_OK);
    static const uint8_t pub_ok[] = { 0x07, 0x00, 0x01, 0x00 };
    check_message("PUBLISH_OK", buf, moq_buf_writer_offset(&w), pub_ok, sizeof(pub_ok));

    /* PUBLISH_OK may carry EXPIRES (9.20.17): 1000 ms is the vi64 83 E8. */
    moq_d21_msg_params_t p;
    memset(&p, 0, sizeof(p));
    p.has_expires = true; p.expires_ms = 1000;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_request_ok(
        &w, MOQ_D21_REQUEST_OK_PUBLISH, &p, NO_BYTES), (int)MOQ_OK);
    static const uint8_t pub_exp[] = { 0x07, 0x00, 0x04, 0x01, 0x08, 0x83, 0xE8 };
    check_message("PUBLISH_OK expires", buf, moq_buf_writer_offset(&w), pub_exp, sizeof(pub_exp));

    /* REQUEST_UPDATE_OK: EXPIRES then LARGEST_OBJECT (+1 delta), 9.20.17/18. */
    memset(&p, 0, sizeof(p));
    p.has_expires = true; p.expires_ms = 1000;
    p.has_largest = true; p.largest_group = 5; p.largest_object = 2;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_request_ok(
        &w, MOQ_D21_REQUEST_OK_REQUEST_UPDATE, &p, NO_BYTES), (int)MOQ_OK);
    static const uint8_t upd_ok[] = { 0x07, 0x00, 0x07, 0x02, 0x08, 0x83, 0xE8, 0x01, 0x05, 0x02 };
    check_message("REQUEST_UPDATE_OK", buf, moq_buf_writer_offset(&w), upd_ok, sizeof(upd_ok));

    moq_control_envelope_t env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_REQUEST_OK);
    moq_d21_request_ok_t ok;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_request_ok(env.payload, env.payload_len,
        MOQ_D21_REQUEST_OK_REQUEST_UPDATE, &ok), (int)MOQ_OK);
    MOQ_TEST_CHECK(ok.params.has_expires && ok.params.expires_ms == 1000);
    MOQ_TEST_CHECK(ok.params.has_largest && ok.params.largest_group == 5 &&
                   ok.params.largest_object == 2);
    MOQ_TEST_CHECK_EQ_SIZE(ok.track_properties.len, 0);

    /* TRACK_STATUS_OK is the one REQUEST_OK that carries Track Properties
     * (9.3): LARGEST_OBJECT {1,0} then DEFAULT_PUBLISHER_PRIORITY. */
    memset(&p, 0, sizeof(p));
    p.has_largest = true; p.largest_group = 1; p.largest_object = 0;
    static const uint8_t props[] = { 0x0E, 0x05 };
    moq_bytes_t tp = { props, sizeof(props) };
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_request_ok(
        &w, MOQ_D21_REQUEST_OK_TRACK_STATUS, &p, tp), (int)MOQ_OK);
    static const uint8_t ts_ok[] = { 0x07, 0x00, 0x06, 0x01, 0x09, 0x01, 0x00, 0x0E, 0x05 };
    check_message("TRACK_STATUS_OK", buf, moq_buf_writer_offset(&w), ts_ok, sizeof(ts_ok));
    env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_REQUEST_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_request_ok(env.payload, env.payload_len,
        MOQ_D21_REQUEST_OK_TRACK_STATUS, &ok), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(ok.track_properties.len, 2);

    /* Track Properties anywhere else are a PROTOCOL_VIOLATION on receipt, and
     * the encoder refuses to send them. */
    static const uint8_t with_props[] = { 0x00, 0x0E, 0x05 };
    static const moq_d21_request_ok_kind_t others[] = {
        MOQ_D21_REQUEST_OK_PUBLISH, MOQ_D21_REQUEST_OK_REQUEST_UPDATE,
        MOQ_D21_REQUEST_OK_SUBSCRIBE_NAMESPACE, MOQ_D21_REQUEST_OK_SUBSCRIBE_TRACKS,
        MOQ_D21_REQUEST_OK_PUBLISH_NAMESPACE };
    for (size_t i = 0; i < sizeof(others) / sizeof(others[0]); i++) {
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_request_ok(with_props, sizeof(with_props),
                                                             others[i], &ok), (int)MOQ_ERR_PROTO);
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_request_ok(&w, others[i], NULL, tp),
                              (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
    }

    /* The parameter set depends on the request answered: PUBLISH_OK takes EXPIRES
     * but not LARGEST_OBJECT; the namespace OKs take EXPIRES only. */
    static const uint8_t largest[] = { 0x01, 0x09, 0x00, 0x00 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_request_ok(largest, sizeof(largest),
        MOQ_D21_REQUEST_OK_PUBLISH, &ok), (int)MOQ_ERR_PROTO);
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_request_ok(largest, sizeof(largest),
        MOQ_D21_REQUEST_OK_SUBSCRIBE_NAMESPACE, &ok), (int)MOQ_ERR_PROTO);
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_request_ok(largest, sizeof(largest),
        MOQ_D21_REQUEST_OK_REQUEST_UPDATE, &ok), (int)MOQ_OK);
    static const uint8_t expires[] = { 0x01, 0x08, 0x00 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_request_ok(expires, sizeof(expires),
        MOQ_D21_REQUEST_OK_PUBLISH_NAMESPACE, &ok), (int)MOQ_OK);

    /* TRACK_STATUS_OK property structure is checked; an unknown Mandatory Track
     * Property is surfaced, not rejected (3.6 lists PUBLISH, SUBSCRIBE_OK and
     * FETCH_OK only). */
    static const uint8_t bad_props[] = { 0x00, 0x01, 0x05, 'a' };   /* odd type, length 5 > 1 */
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_request_ok(bad_props, sizeof(bad_props),
        MOQ_D21_REQUEST_OK_TRACK_STATUS, &ok), (int)MOQ_ERR_PROTO);
    static const uint8_t mand[] = { 0x00, 0xC0, 0x40, 0x00, 0x00 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_request_ok(mand, sizeof(mand),
        MOQ_D21_REQUEST_OK_TRACK_STATUS, &ok), (int)MOQ_OK);

    /* Truncated: a count with nothing after it. */
    static const uint8_t trunc[] = { 0x01 };
    MOQ_TEST_CHECK(moq_d21_decode_request_ok(trunc, sizeof(trunc),
        MOQ_D21_REQUEST_OK_PUBLISH, &ok) < 0);
}

static void t_request_error(void)
{
    uint8_t buf[128];
    moq_buf_writer_t w;

    /* 9.4.2: NOT_SUPPORTED (0x03), Retry Interval 0, reason "no". */
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_request_error(&w, 0x03, 0,
        MOQ_BYTES_LITERAL("no")), (int)MOQ_OK);
    static const uint8_t ne[] = { 0x05, 0x00, 0x05, 0x03, 0x00, 0x02, 'n', 'o' };
    check_message("REQUEST_ERROR", buf, moq_buf_writer_offset(&w), ne, sizeof(ne));
    moq_control_envelope_t env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_REQUEST_ERROR);
    moq_d21_request_error_t e;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_request_error(env.payload, env.payload_len, &e),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(e.error_code, 0x03);
    MOQ_TEST_CHECK_EQ_U64(e.retry_interval, 0);
    MOQ_TEST_CHECK(e.reason.len == 2 && memcmp(e.reason.data, "no", 2) == 0);

    /* INVALID_FILTER (0x36) is new in draft 21 (16.11.2) and Retry Interval is a
     * raw "milliseconds plus one" integer: 1 means retry immediately. */
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_request_error(&w, MOQ_D21_ERROR_INVALID_FILTER, 1,
        NO_BYTES), (int)MOQ_OK);
    static const uint8_t inf[] = { 0x05, 0x00, 0x03, 0x36, 0x01, 0x00 };
    check_message("REQUEST_ERROR INVALID_FILTER", buf, moq_buf_writer_offset(&w), inf, sizeof(inf));

    /* 9.4.1: the REDIRECT code carries a Redirect: empty URI (this session),
     * namespace [a], track name t. */
    moq_bytes_t rparts[] = { MOQ_BYTES_LITERAL("a") };
    moq_d21_redirect_t red;
    memset(&red, 0, sizeof(red));
    red.track_namespace.parts = rparts; red.track_namespace.count = 1;
    red.track_name = MOQ_BYTES_LITERAL("t");
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_request_error_redirect(&w, MOQ_D21_ERROR_REDIRECT,
        0, NO_BYTES, &red), (int)MOQ_OK);
    static const uint8_t rd[] = { 0x05, 0x00, 0x09, 0x34, 0x00, 0x00, 0x00, 0x01, 0x01, 'a', 0x01, 't' };
    check_message("REQUEST_ERROR REDIRECT", buf, moq_buf_writer_offset(&w), rd, sizeof(rd));
    env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_REQUEST_ERROR);
    moq_bytes_t dparts[32];
    moq_d21_redirect_t got;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_request_error_redirect(env.payload, env.payload_len,
        dparts, 32, &e, &got), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(e.error_code, MOQ_D21_ERROR_REDIRECT);
    MOQ_TEST_CHECK_EQ_SIZE(got.connect_uri.len, 0);
    MOQ_TEST_CHECK_EQ_SIZE(got.track_namespace.count, 1);
    MOQ_TEST_CHECK(got.track_name.len == 1 && got.track_name.data[0] == 't');

    /* A REDIRECT code with no Redirect is malformed, so the plain encoder
     * refuses it and the plain decoder does not accept it. */
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_request_error(&w, MOQ_D21_ERROR_REDIRECT, 0,
        NO_BYTES), (int)MOQ_ERR_INVAL);
    static const uint8_t bare_redirect[] = { 0x34, 0x00, 0x00 };
    MOQ_TEST_CHECK(moq_d21_decode_request_error(bare_redirect, sizeof(bare_redirect), &e) < 0);
    MOQ_TEST_CHECK(moq_d21_decode_request_error_redirect(bare_redirect, sizeof(bare_redirect),
        dparts, 32, &e, &got) < 0);

    /* A Redirect after any other code is trailing garbage. */
    static const uint8_t extra[] = { 0x03, 0x00, 0x00, 0x00, 0x01, 0x01, 'a', 0x01, 't' };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_request_error_redirect(extra, sizeof(extra),
        dparts, 32, &e, &got), (int)MOQ_ERR_PROTO);

    /* The reason phrase is at most 1024 bytes (8.5): 1025 closes on receipt and is
     * refused on send; 1024 is accepted. */
    static uint8_t reason[1025];
    memset(reason, 'x', sizeof(reason));
    static uint8_t big[1100];
    moq_buf_writer_t bw;
    moq_buf_writer_init(&bw, big, sizeof(big));
    moq_bytes_t r1024 = { reason, 1024 }, r1025 = { reason, 1025 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_request_error(&bw, 0x01, 0, r1024), (int)MOQ_OK);
    moq_buf_writer_init(&bw, big, sizeof(big));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_request_error(&bw, 0x01, 0, r1025), (int)MOQ_ERR_INVAL);
    {
        uint8_t body[1040];
        size_t bn = 0;
        body[bn++] = 0x01; body[bn++] = 0x00;
        body[bn++] = 0x84; body[bn++] = 0x01;           /* length 1025 as a vi64 */
        memcpy(body + bn, reason, 1025); bn += 1025;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_request_error(body, bn, &e), (int)MOQ_ERR_PROTO);
    }
}

/* The code registries (16.11.1 to 16.11.3): the numbers are the wire contract, so
 * they are pinned here against the IANA tables, including what draft 21 removed
 * and added relative to draft 18. */
static void t_code_registries(void)
{
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_ERROR_INTERNAL_ERROR, 0x0);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_ERROR_UNAUTHORIZED, 0x1);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_ERROR_TIMEOUT, 0x2);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_ERROR_NOT_SUPPORTED, 0x3);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_ERROR_MALFORMED_AUTH_TOKEN, 0x4);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_ERROR_EXPIRED_AUTH_TOKEN, 0x5);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_ERROR_GOING_AWAY, 0x6);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_ERROR_EXCESSIVE_LOAD, 0x9);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_ERROR_DOES_NOT_EXIST, 0x10);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_ERROR_INVALID_RANGE, 0x11);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_ERROR_MALFORMED_TRACK, 0x12);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_ERROR_UNINTERESTED, 0x20);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_ERROR_PREFIX_OVERLAP, 0x30);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_ERROR_NAMESPACE_TOO_LARGE, 0x31);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_ERROR_UNSUPPORTED_EXTENSION, 0x33);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_ERROR_REDIRECT, 0x34);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_ERROR_CONFLICTING_FILTERS, 0x35);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_ERROR_INVALID_FILTER, 0x36);

    /* Registered codes, and the two draft 18 codes that no longer are. */
    MOQ_TEST_CHECK(moq_d21_request_error_registered(0x36));
    MOQ_TEST_CHECK(moq_d21_request_error_registered(0x35));
    MOQ_TEST_CHECK(!moq_d21_request_error_registered(0x19));   /* DUPLICATE_SUBSCRIPTION */
    MOQ_TEST_CHECK(!moq_d21_request_error_registered(0x32));   /* INVALID_JOINING_REQUEST_ID */
    MOQ_TEST_CHECK(!moq_d21_request_error_registered(0x7));
    MOQ_TEST_CHECK(!moq_d21_request_error_registered(0x9D));   /* GREASE: never registered */
    MOQ_TEST_CHECK(!moq_d21_request_error_registered(UINT64_MAX));

    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_PUBLISH_DONE_INTERNAL_ERROR, 0x0);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_PUBLISH_DONE_UNAUTHORIZED, 0x1);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_PUBLISH_DONE_TRACK_ENDED, 0x2);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_PUBLISH_DONE_GOING_AWAY, 0x4);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_PUBLISH_DONE_TOO_FAR_BEHIND, 0x5);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_PUBLISH_DONE_EXPIRED, 0x6);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_PUBLISH_DONE_UPDATE_FAILED, 0x8);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_PUBLISH_DONE_EXCESSIVE_LOAD, 0x9);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_PUBLISH_DONE_MALFORMED_TRACK, 0x12);
    MOQ_TEST_CHECK(moq_d21_publish_done_registered(0x8));
    MOQ_TEST_CHECK(!moq_d21_publish_done_registered(0x3));     /* SUBSCRIPTION_ENDED removed */
    MOQ_TEST_CHECK(!moq_d21_publish_done_registered(0x3F));

    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_SESSION_ERR_TOO_MANY_REQUEST_UPDATES, 0x1B);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_SESSION_ERR_PROTOCOL_VIOLATION, 0x3);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_SESSION_ERR_KEY_VALUE_FORMATTING, 0x6);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_SESSION_ERR_INVALID_AUTHORITY, 0x19);
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_SESSION_ERR_MALFORMED_AUTHORITY, 0x1A);
    MOQ_TEST_CHECK(moq_d21_session_error_registered(0x1B));
    MOQ_TEST_CHECK(!moq_d21_session_error_registered(0x15));   /* VERSION_NEGOTIATION_FAILED removed */
}


/* == 4d. PUBLISH, PUBLISH_DONE, PUBLISH_STATE_NOTIFY, PUBLISH_SKIPPED ====== */

static void t_publish(void)
{
    /* 9.8: Request ID 7, namespace [a], name t, alias 9. Draft 21 moves the
     * subscription parameters onto PUBLISH itself (they are no longer in
     * PUBLISH_OK): LARGEST_OBJECT {4,2}, FORWARD 0, SUBSCRIBER_PRIORITY 100,
     * GROUP_ORDER 1, then a Track Property (DEFAULT_PUBLISHER_PRIORITY = 10). */
    uint8_t buf[96];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("a") };
    static const uint8_t props[] = { 0x0E, 0x0A };
    moq_d21_publish_t p;
    memset(&p, 0, sizeof(p));
    p.request_id = 7;
    p.track_namespace.parts = parts; p.track_namespace.count = 1;
    p.track_name = MOQ_BYTES_LITERAL("t");
    p.track_alias = 9;
    p.params.has_largest = true; p.params.largest_group = 4; p.params.largest_object = 2;
    p.params.has_forward = true; p.params.forward = 0;
    p.params.has_subscriber_priority = true; p.params.subscriber_priority = 100;
    p.params.has_group_order = true; p.params.group_order = 1;
    p.track_properties.data = props; p.track_properties.len = sizeof(props);
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_publish(&w, &p), (int)MOQ_OK);
    static const uint8_t want[] = {
        0x1D, 0x00, 0x13, 0x07, 0x01, 0x01, 'a', 0x01, 't', 0x09,
        0x04, 0x09, 0x04, 0x02, 0x07, 0x00, 0x10, 0x64, 0x02, 0x01,
        0x0E, 0x0A };
    check_bytes("PUBLISH", buf, moq_buf_writer_offset(&w), want, sizeof(want));

    moq_control_envelope_t env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_PUBLISH);
    moq_bytes_t dparts[32];
    moq_d21_publish_t g;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish(env.payload, env.payload_len,
                                                      dparts, 32, &g), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(g.request_id, 7);
    MOQ_TEST_CHECK_EQ_U64(g.track_alias, 9);
    MOQ_TEST_CHECK(g.params.has_largest && g.params.largest_group == 4 &&
                   g.params.largest_object == 2);
    MOQ_TEST_CHECK(g.params.has_forward && g.params.forward == 0);
    MOQ_TEST_CHECK(g.params.has_subscriber_priority && g.params.subscriber_priority == 100);
    MOQ_TEST_CHECK(g.params.has_group_order && g.params.group_order == 1);
    MOQ_TEST_CHECK_EQ_SIZE(g.track_properties.len, 2);
    MOQ_TEST_CHECK(!g.dynamic_groups && !g.track_properties_unsupported);

    /* DYNAMIC_GROUPS and a Mandatory Track Property are surfaced (10.6, 3.6). */
    static const uint8_t dyn[] = { 0x07, 0x01, 0x01, 'a', 0x01, 't', 0x09, 0x00, 0x30, 0x01 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish(dyn, sizeof(dyn), dparts, 32, &g), (int)MOQ_OK);
    MOQ_TEST_CHECK(g.dynamic_groups);
    static const uint8_t mand[] = { 0x07, 0x01, 0x01, 'a', 0x01, 't', 0x09, 0x00, 0xC0, 0x40, 0x00, 0x00 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish(mand, sizeof(mand), dparts, 32, &g), (int)MOQ_OK);
    MOQ_TEST_CHECK(g.track_properties_unsupported);

    /* PUBLISH does not take FILL_PARAMETERS or INCLUDE_PROPERTIES: those are
     * requests a subscriber makes (9.20.16, 9.20.22). */
    static const uint8_t fill[] = { 0x07, 0x01, 0x01, 'a', 0x01, 't', 0x09, 0x01, 0x23, 0x00 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish(fill, sizeof(fill), dparts, 32, &g), (int)MOQ_ERR_PROTO);
    memset(&p.params, 0, sizeof(p.params));
    p.params.has_include_properties = true;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_publish(&w, &p), (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);

    /* A Full Track Name over 4096 bytes closes on receipt (8.7). */
    {
        static uint8_t big[4200];
        size_t n = 0;
        big[n++] = 0x07; big[n++] = 0x01; big[n++] = 0x01; big[n++] = 'a';
        big[n++] = 0x90; big[n++] = 0x01;            /* name length 4097 = 0x1001 -> 2-byte vi64 */
        big[n - 2] = 0x90; big[n - 1] = 0x01;
        memset(big + n, 'x', 4097); n += 4097;
        big[n++] = 0x09; big[n++] = 0x00;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish(big, n, dparts, 32, &g), (int)MOQ_ERR_PROTO);
    }
    /* Malformed Track Properties are refused on send and fail the decode. */
    {
        static const uint8_t badp[] = { 0x01, 0x05, 'a' };     /* odd type, length 5 > 1 */
        p.track_properties.data = badp; p.track_properties.len = sizeof(badp);
        memset(&p.params, 0, sizeof(p.params));
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_publish(&w, &p), (int)MOQ_ERR_INVAL);
    }
}

static void t_publish_done(void)
{
    uint8_t buf[64];
    moq_buf_writer_t w;

    /* 9.9: UPDATE_FAILED (0x08), 3 streams, reason "x". */
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_publish_done(&w, MOQ_D21_PUBLISH_DONE_UPDATE_FAILED,
        3, MOQ_BYTES_LITERAL("x")), (int)MOQ_OK);
    static const uint8_t want[] = { 0x0B, 0x00, 0x04, 0x08, 0x03, 0x01, 'x' };
    check_bytes("PUBLISH_DONE", buf, moq_buf_writer_offset(&w), want, sizeof(want));
    moq_control_envelope_t env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_PUBLISH_DONE);
    moq_d21_publish_done_t d;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish_done(env.payload, env.payload_len, &d), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(d.status_code, 8);
    MOQ_TEST_CHECK_EQ_U64(d.stream_count, 3);
    MOQ_TEST_CHECK(d.reason.len == 1 && d.reason.data[0] == 'x');

    /* Stream Count is a full-range vi64: 2^64-1 means "could not count" (9.9). */
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_publish_done(&w, MOQ_D21_PUBLISH_DONE_TRACK_ENDED,
        UINT64_MAX, NO_BYTES), (int)MOQ_OK);
    static const uint8_t maxc[] = { 0x0B, 0x00, 0x0B, 0x02,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00 };
    check_bytes("PUBLISH_DONE max stream count", buf, moq_buf_writer_offset(&w), maxc, sizeof(maxc));
    env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_PUBLISH_DONE);
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish_done(env.payload, env.payload_len, &d), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(d.stream_count, UINT64_MAX);

    /* The codec carries any status value: an unregistered one (SUBSCRIPTION_ENDED
     * was removed, and GREASE is reserved) is handled by the session as
     * INTERNAL_ERROR, never by closing (13). */
    static const uint8_t ended[] = { 0x03, 0x00, 0x00 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish_done(ended, sizeof(ended), &d), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(d.status_code, 3);
    MOQ_TEST_CHECK(!moq_d21_publish_done_registered(d.status_code));

    /* Trailing bytes and an over-long reason (8.5) are protocol violations. */
    static const uint8_t extra[] = { 0x02, 0x00, 0x00, 0x00 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish_done(extra, sizeof(extra), &d), (int)MOQ_ERR_PROTO);
    static uint8_t reason[1025];
    memset(reason, 'r', sizeof(reason));
    static uint8_t body[1040];
    size_t bn = 0;
    body[bn++] = 0x02; body[bn++] = 0x00; body[bn++] = 0x84; body[bn++] = 0x01;
    memcpy(body + bn, reason, 1025); bn += 1025;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish_done(body, bn, &d), (int)MOQ_ERR_PROTO);
    moq_bytes_t r1025 = { reason, 1025 };
    moq_buf_writer_t bw;
    static uint8_t big[1100];
    moq_buf_writer_init(&bw, big, sizeof(big));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_publish_done(&bw, 0, 0, r1025), (int)MOQ_ERR_INVAL);
}

static void t_publish_state_notify(void)
{
    uint8_t buf[64];
    moq_buf_writer_t w;

    /* 9.10: type 0x22, no Request ID, a bare parameter block. FORWARD = 0. */
    moq_d21_msg_params_t p;
    memset(&p, 0, sizeof(p));
    p.has_forward = true; p.forward = 0;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_publish_state_notify(&w, &p), (int)MOQ_OK);
    static const uint8_t fwd[] = { 0x22, 0x00, 0x03, 0x01, 0x10, 0x00 };
    check_bytes("PUBLISH_STATE_NOTIFY forward", buf, moq_buf_writer_offset(&w), fwd, sizeof(fwd));

    /* The three parameters it may carry (9.20.10, 9.20.18, 9.20.19), with the
     * LARGEST_OBJECT the draft requires the publisher to include when known. */
    memset(&p, 0, sizeof(p));
    p.has_largest = true; p.largest_group = 9; p.largest_object = 1;
    p.has_forward = true; p.forward = 1;
    p.has_location_filter = true; p.location_filter.field_count = 2;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_publish_state_notify(&w, &p), (int)MOQ_OK);
    static const uint8_t all[] = { 0x22, 0x00, 0x0A, 0x03,
        0x09, 0x09, 0x01, 0x07, 0x01, 0x11, 0x02, 0x00, 0x00 };
    check_bytes("PUBLISH_STATE_NOTIFY all", buf, moq_buf_writer_offset(&w), all, sizeof(all));
    moq_control_envelope_t env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_PUBLISH_STATE_NOTIFY);
    moq_d21_publish_state_notify_t n;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish_state_notify(env.payload, env.payload_len, &n), (int)MOQ_OK);
    MOQ_TEST_CHECK(n.params.has_largest && n.params.largest_group == 9);
    MOQ_TEST_CHECK(n.params.has_forward && n.params.forward == 1);
    MOQ_TEST_CHECK(n.params.has_location_filter && n.params.location_filter.field_count == 2);

    /* An empty notification is valid, and nothing may follow the parameters. */
    static const uint8_t empty[] = { 0x00 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish_state_notify(empty, sizeof(empty), &n), (int)MOQ_OK);
    static const uint8_t trailing[] = { 0x00, 0x0E, 0x05 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish_state_notify(trailing, sizeof(trailing), &n),
                          (int)MOQ_ERR_PROTO);

    /* Subscriber-controlled parameters other than FORWARD and the filter are not
     * allowed (a publisher must not change them), nor are fills or tokens. */
    static const uint8_t prio[] = { 0x01, 0x20, 0x05 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish_state_notify(prio, sizeof(prio), &n), (int)MOQ_ERR_PROTO);
    memset(&p, 0, sizeof(p));
    p.has_subscriber_priority = true;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_publish_state_notify(&w, &p), (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_publish_state_notify(&w, NULL), (int)MOQ_ERR_INVAL);
}

static void t_publish_skipped(void)
{
    uint8_t buf[64];
    moq_buf_writer_t w;

    /* 9.19: suffix [a], track name t. Renamed from PUBLISH_BLOCKED; same bytes. */
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("a") };
    moq_namespace_t suffix = { parts, 1 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_publish_skipped(&w, &suffix, MOQ_BYTES_LITERAL("t")),
                          (int)MOQ_OK);
    static const uint8_t want[] = { 0x0F, 0x00, 0x05, 0x01, 0x01, 'a', 0x01, 't' };
    check_bytes("PUBLISH_SKIPPED", buf, moq_buf_writer_offset(&w), want, sizeof(want));
    MOQ_TEST_CHECK_EQ_U64(MOQ_D21_PUBLISH_SKIPPED, 0x0F);

    moq_control_envelope_t env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_PUBLISH_SKIPPED);
    moq_bytes_t dparts[32];
    moq_d21_publish_skipped_t g;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish_skipped(env.payload, env.payload_len,
                                                              dparts, 32, &g), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(g.track_namespace_suffix.count, 1);
    MOQ_TEST_CHECK(g.track_name.len == 1 && g.track_name.data[0] == 't');

    /* A zero-field suffix is valid (the track sits directly under the prefix). */
    static const uint8_t root[] = { 0x00, 0x01, 't' };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish_skipped(root, sizeof(root), dparts, 32, &g), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(g.track_namespace_suffix.count, 0);
    /* 33 fields close the session (8.7). */
    uint8_t many[120];
    size_t n = 0;
    many[n++] = 33;
    for (int i = 0; i < 33; i++) { many[n++] = 0x01; many[n++] = 'x'; }
    many[n++] = 0x01; many[n++] = 't';
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish_skipped(many, n, dparts, 40, &g), (int)MOQ_ERR_PROTO);
}


/* == 4e. FETCH, FETCH_OK, FETCH_HEADER ===================================== */

static void t_fetch(void)
{
    /* 9.11: Request ID 4, namespace [a], name t. There is no fetch type and no
     * start or end field: the range travels in LOCATION_FILTER ({3,0} to group
     * 3+2 object 7) beside SUBSCRIBER_PRIORITY 5. */
    uint8_t buf[96];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("a") };
    moq_d21_fetch_t f;
    memset(&f, 0, sizeof(f));
    f.request_id = 4;
    f.track_namespace.parts = parts; f.track_namespace.count = 1;
    f.track_name = MOQ_BYTES_LITERAL("t");
    f.params.has_subscriber_priority = true; f.params.subscriber_priority = 5;
    f.params.has_location_filter = true;
    f.params.location_filter.field_count = 4;
    f.params.location_filter.start_group = 3; f.params.location_filter.start_object = 0;
    f.params.location_filter.end_group_delta = 2; f.params.location_filter.end_object = 7;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_fetch(&w, &f), (int)MOQ_OK);
    static const uint8_t want[] = {
        0x16, 0x00, 0x0F, 0x04, 0x01, 0x01, 'a', 0x01, 't',
        0x02, 0x20, 0x05, 0x01, 0x04, 0x03, 0x00, 0x02, 0x07 };
    check_bytes("FETCH", buf, moq_buf_writer_offset(&w), want, sizeof(want));

    moq_control_envelope_t env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_FETCH);
    moq_bytes_t dparts[32];
    moq_d21_fetch_t g;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_fetch(env.payload, env.payload_len, dparts, 32, &g),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(g.request_id, 4);
    MOQ_TEST_CHECK_EQ_SIZE(g.track_namespace.count, 1);
    MOQ_TEST_CHECK(g.track_name.len == 1 && g.track_name.data[0] == 't');
    MOQ_TEST_CHECK(g.params.has_subscriber_priority && g.params.subscriber_priority == 5);
    MOQ_TEST_CHECK(g.params.has_location_filter && g.params.location_filter.field_count == 4 &&
                   g.params.location_filter.end_object == 7);

    /* A fetch with no filter covers {0,0} to Largest Object (3.3.1); the body is
     * just the identity and an empty parameter block. */
    memset(&f.params, 0, sizeof(f.params));
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_fetch(&w, &f), (int)MOQ_OK);
    static const uint8_t bare[] = { 0x16, 0x00, 0x07, 0x04, 0x01, 0x01, 'a', 0x01, 't', 0x00 };
    check_bytes("FETCH bare", buf, moq_buf_writer_offset(&w), bare, sizeof(bare));

    /* Its own parameters: GROUP_ORDER, FILL_TIMEOUT, INCLUDE_PROPERTIES, a token. */
    memset(&f.params, 0, sizeof(f.params));
    f.params.has_fill_timeout = true; f.params.fill_timeout_ms = 50;
    f.params.has_group_order = true; f.params.group_order = 2;
    f.params.has_include_properties = true; f.params.include_properties = 0;
    f.params.auth_token_count = 1;
    f.params.auth_tokens[0].alias_type = 3;            /* USE_VALUE */
    f.params.auth_tokens[0].token_type = 1;
    f.params.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("k");
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_fetch(&w, &f), (int)MOQ_OK);
    env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_FETCH);
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_fetch(env.payload, env.payload_len, dparts, 32, &g),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(g.params.has_fill_timeout && g.params.fill_timeout_ms == 50);
    MOQ_TEST_CHECK(g.params.has_group_order && g.params.group_order == 2);
    MOQ_TEST_CHECK(g.params.has_include_properties && g.params.include_properties == 0);
    MOQ_TEST_CHECK_EQ_SIZE(g.params.auth_token_count, 1);

    /* Parameters FETCH does not take: FORWARD (a subscription state) and
     * FILL_PARAMETERS (a request for a fill) are violations on the wire and
     * refused on send. */
    static const uint8_t fwd[] = { 0x04, 0x01, 0x01, 'a', 0x01, 't', 0x01, 0x10, 0x01 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_fetch(fwd, sizeof(fwd), dparts, 32, &g), (int)MOQ_ERR_PROTO);
    static const uint8_t fill[] = { 0x04, 0x01, 0x01, 'a', 0x01, 't', 0x01, 0x23, 0x00 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_fetch(fill, sizeof(fill), dparts, 32, &g), (int)MOQ_ERR_PROTO);
    memset(&f.params, 0, sizeof(f.params));
    f.params.has_forward = true;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_fetch(&w, &f), (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);

    /* Range Filters are permitted on FETCH and are counted, not acted on. */
    static const uint8_t rf[] = { 0x04, 0x01, 0x01, 'a', 0x01, 't', 0x01, 0x26, 0x02, 0x00, 0x05 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_fetch(rf, sizeof(rf), dparts, 32, &g), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)g.params.range_filter_params, 1);
    MOQ_TEST_CHECK_EQ_U64(g.params.range_filter_ranges, 1);

    /* Trailing bytes after the parameters, and a Full Track Name over 4096 bytes. */
    static const uint8_t trail[] = { 0x04, 0x01, 0x01, 'a', 0x01, 't', 0x00, 0x00 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_fetch(trail, sizeof(trail), dparts, 32, &g), (int)MOQ_ERR_PROTO);
    {
        static uint8_t big[4200];
        size_t n = 0;
        big[n++] = 0x04; big[n++] = 0x01; big[n++] = 0x01; big[n++] = 'a';
        big[n++] = 0x90; big[n++] = 0x01;                    /* name length 4097 */
        memset(big + n, 'x', 4097); n += 4097;
        big[n++] = 0x00;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_fetch(big, n, dparts, 32, &g), (int)MOQ_ERR_PROTO);
    }
}

/* A draft-18 FETCH must never be mistaken for a draft-21 one. Draft 18 put a
 * Fetch Type (1 standalone, 2 relative joining, 3 absolute joining) after the
 * Request ID; draft 21 removed it. Both legacy encodings are fed to the draft-21
 * decoder and must fail rather than decode as some other request. */
static void t_fetch_rejects_draft18_forms(void)
{
    moq_bytes_t dparts[32];
    moq_d21_fetch_t g;
    /* Standalone: Request ID 4, type 1, namespace [a], name t, start {0,0}, end
     * {0,0}, zero parameters. */
    static const uint8_t standalone[] = {
        0x04, 0x01, 0x01, 0x01, 'a', 0x01, 't', 0x00, 0x00, 0x00, 0x00, 0x00 };
    MOQ_TEST_CHECK(moq_d21_decode_fetch(standalone, sizeof(standalone), dparts, 32, &g) < 0);
    /* Relative joining: Request ID 4, type 2, joining request 0, start 1, no params. */
    static const uint8_t joining[] = { 0x04, 0x02, 0x00, 0x01, 0x00 };
    MOQ_TEST_CHECK(moq_d21_decode_fetch(joining, sizeof(joining), dparts, 32, &g) < 0);
    /* Absolute joining: type 3. */
    static const uint8_t absolute[] = { 0x04, 0x03, 0x00, 0x01, 0x00 };
    MOQ_TEST_CHECK(moq_d21_decode_fetch(absolute, sizeof(absolute), dparts, 32, &g) < 0);
}

static void t_fetch_ok(void)
{
    /* 9.12: End Of Track 0, End Location {5,2}, zero parameters, and a Track
     * Property (DEFAULT_PUBLISHER_PRIORITY = 5) running to the end. */
    uint8_t buf[64];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    static const uint8_t props[] = { 0x0E, 0x05 };
    moq_bytes_t tp = { props, sizeof(props) };
    moq_d21_location_t end = { 5, 2 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_fetch_ok(&w, false, end, tp), (int)MOQ_OK);
    static const uint8_t want[] = { 0x18, 0x00, 0x06, 0x00, 0x05, 0x02, 0x00, 0x0E, 0x05 };
    check_bytes("FETCH_OK", buf, moq_buf_writer_offset(&w), want, sizeof(want));

    moq_control_envelope_t env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_FETCH_OK);
    moq_d21_fetch_ok_t ok;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_fetch_ok(env.payload, env.payload_len, &ok), (int)MOQ_OK);
    MOQ_TEST_CHECK(!ok.end_of_track && ok.end.group == 5 && ok.end.object == 2);
    MOQ_TEST_CHECK_EQ_SIZE(ok.track_properties.len, 2);
    MOQ_TEST_CHECK(!ok.track_properties_unsupported);

    /* End Of Track is one byte and only 0 or 1 carry meaning. */
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_fetch_ok(&w, true, end, NO_BYTES), (int)MOQ_OK);
    static const uint8_t eot[] = { 0x18, 0x00, 0x04, 0x01, 0x05, 0x02, 0x00 };
    check_bytes("FETCH_OK end of track", buf, moq_buf_writer_offset(&w), eot, sizeof(eot));
    static const uint8_t bad_eot[] = { 0x02, 0x05, 0x02, 0x00 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_fetch_ok(bad_eot, sizeof(bad_eot), &ok), (int)MOQ_ERR_PROTO);

    /* No parameter is defined for FETCH_OK (9.20), so any is a violation. */
    static const uint8_t with_param[] = { 0x00, 0x05, 0x02, 0x01, 0x08, 0x00 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_fetch_ok(with_param, sizeof(with_param), &ok), (int)MOQ_ERR_PROTO);

    /* An unknown Mandatory Track Property is surfaced so the subscriber can
     * cancel the fetch (3.6); a malformed property block is a violation. */
    static const uint8_t mand[] = { 0x00, 0x05, 0x02, 0x00, 0xC0, 0x40, 0x00, 0x00 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_fetch_ok(mand, sizeof(mand), &ok), (int)MOQ_OK);
    MOQ_TEST_CHECK(ok.track_properties_unsupported);
    static const uint8_t bad_props[] = { 0x00, 0x05, 0x02, 0x00, 0x01, 0x09, 'a' };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_fetch_ok(bad_props, sizeof(bad_props), &ok), (int)MOQ_ERR_PROTO);

    /* The encoder refuses malformed properties and leaves the writer alone. */
    static const uint8_t badp[] = { 0x01, 0x09, 'a' };
    moq_bytes_t bp = { badp, sizeof(badp) };
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_fetch_ok(&w, false, end, bp), (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
}

static void t_fetch_header(void)
{
    /* 11.4.1: the fetch data stream starts with type 0x05 and the Request ID. */
    uint8_t buf[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_fetch_header(&w, 4), (int)MOQ_OK);
    static const uint8_t want[] = { 0x05, 0x04 };
    check_bytes("FETCH_HEADER", buf, moq_buf_writer_offset(&w), want, sizeof(want));

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
    uint64_t id = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_fetch_header(&r, &id), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(id, 4);

    /* A different stream type is a violation and leaves the reader where it was,
     * so the caller can still classify the stream. */
    static const uint8_t sg[] = { 0x10, 0x01 };
    moq_buf_reader_init(&r, sg, sizeof(sg));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_fetch_header(&r, &id), (int)MOQ_ERR_PROTO);
    MOQ_TEST_CHECK_EQ_SIZE(r.pos, 0);
    /* An incomplete header also leaves the reader alone. */
    static const uint8_t half[] = { 0x05 };
    moq_buf_reader_init(&r, half, sizeof(half));
    MOQ_TEST_CHECK(moq_d21_decode_fetch_header(&r, &id) < 0);
    MOQ_TEST_CHECK_EQ_SIZE(r.pos, 0);
    /* The Request ID is a full-range vi64. */
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_fetch_header(&w, UINT64_MAX), (int)MOQ_OK);
    moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_fetch_header(&r, &id), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(id, UINT64_MAX);
}


/* == 4f. GOAWAY, TRACK_STATUS, namespace messages, SUBSCRIBE_TRACKS ======== */

static void t_goaway(void)
{
    uint8_t buf[64];
    moq_buf_writer_t w;

    /* 9.2: New Session URI Length, URI, Timeout; no Request ID in either the
     * control-stream or the request-stream form. Empty URI, 1000 ms. */
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_goaway(&w, NULL, 0, 1000), (int)MOQ_OK);
    static const uint8_t empty[] = { 0x10, 0x00, 0x03, 0x00, 0x83, 0xE8 };
    check_bytes("GOAWAY empty URI", buf, moq_buf_writer_offset(&w), empty, sizeof(empty));

    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_goaway(&w, (const uint8_t *)"x", 1, 0), (int)MOQ_OK);
    static const uint8_t with_uri[] = { 0x10, 0x00, 0x03, 0x01, 'x', 0x00 };
    check_bytes("GOAWAY with URI", buf, moq_buf_writer_offset(&w), with_uri, sizeof(with_uri));

    moq_control_envelope_t env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_GOAWAY);
    moq_d21_goaway_t g;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_goaway(env.payload, env.payload_len, &g), (int)MOQ_OK);
    MOQ_TEST_CHECK(g.uri.len == 1 && g.uri.data[0] == 'x');
    MOQ_TEST_CHECK_EQ_U64(g.timeout_ms, 0);

    /* Timeout is a full-range vi64. */
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_goaway(&w, NULL, 0, UINT64_MAX), (int)MOQ_OK);
    env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_GOAWAY);
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_goaway(env.payload, env.payload_len, &g), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(g.timeout_ms, UINT64_MAX);

    /* The URI is at most 8192 bytes (9.2): 8192 passes, 8193 is refused on send
     * and a violation on receipt. */
    static uint8_t uri[8200];
    memset(uri, 'u', sizeof(uri));
    static uint8_t big[8300];
    moq_buf_writer_t bw;
    moq_buf_writer_init(&bw, big, sizeof(big));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_goaway(&bw, uri, 8192, 1), (int)MOQ_OK);
    moq_buf_writer_init(&bw, big, sizeof(big));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_goaway(&bw, uri, 8193, 1), (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&bw), 0);
    {
        static uint8_t body[8210];
        size_t n = 0;
        body[n++] = 0xA0; body[n++] = 0x01;          /* length 8193 */
        memcpy(body + n, uri, 8193); n += 8193;
        body[n++] = 0x00;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_goaway(body, n, &g), (int)MOQ_ERR_PROTO);
    }

    /* Draft 18's control-stream GOAWAY ended with a Request ID: URI (empty),
     * Timeout 5, Request ID 3. Draft 21 has no such field, so the extra byte is
     * trailing data and the message is a violation, not a GOAWAY about request 3. */
    static const uint8_t d18_form[] = { 0x00, 0x05, 0x03 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_goaway(d18_form, sizeof(d18_form), &g), (int)MOQ_ERR_PROTO);
    /* A truncated body is not a GOAWAY either. */
    static const uint8_t trunc[] = { 0x00 };
    MOQ_TEST_CHECK(moq_d21_decode_goaway(trunc, sizeof(trunc), &g) < 0);
    static const uint8_t short_uri[] = { 0x05, 'a', 'b' };
    MOQ_TEST_CHECK(moq_d21_decode_goaway(short_uri, sizeof(short_uri), &g) < 0);
    /* A length with no data behind it is refused, not read from NULL. */
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_goaway(&w, NULL, 3, 0), (int)MOQ_ERR_INVAL);
}

static void t_track_status(void)
{
    /* 9.13: the SUBSCRIBE layout. Request ID 2, namespace [a], name t,
     * INCLUDE_PROPERTIES = 0. Only AUTHORIZATION_TOKEN and INCLUDE_PROPERTIES
     * are defined for it. */
    uint8_t buf[64];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("a") };
    moq_namespace_t ns = { parts, 1 };
    moq_d21_msg_params_t p;
    memset(&p, 0, sizeof(p));
    p.has_include_properties = true; p.include_properties = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_track_status(&w, 2, &ns, MOQ_BYTES_LITERAL("t"), &p),
                          (int)MOQ_OK);
    static const uint8_t want[] = { 0x0D, 0x00, 0x09, 0x02, 0x01, 0x01, 'a', 0x01, 't', 0x01, 0x35, 0x00 };
    check_bytes("TRACK_STATUS", buf, moq_buf_writer_offset(&w), want, sizeof(want));

    moq_control_envelope_t env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_TRACK_STATUS);
    moq_bytes_t dparts[32];
    moq_d21_track_status_t g;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_track_status(env.payload, env.payload_len, dparts, 32, &g),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(g.request_id, 2);
    MOQ_TEST_CHECK(g.params.has_include_properties && g.params.include_properties == 0);

    /* Delivery parameters belong to subscriptions, not status (9.13). */
    static const uint8_t prio[] = { 0x02, 0x01, 0x01, 'a', 0x01, 't', 0x01, 0x20, 0x05 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_track_status(prio, sizeof(prio), dparts, 32, &g), (int)MOQ_ERR_PROTO);
    memset(&p, 0, sizeof(p));
    p.has_subscriber_priority = true;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_track_status(&w, 2, &ns, MOQ_BYTES_LITERAL("t"), &p),
                          (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
}

static void t_namespace_family(void)
{
    uint8_t buf[96];
    moq_buf_writer_t w;
    moq_bytes_t dparts[40];
    moq_d21_msg_params_t none;
    memset(&none, 0, sizeof(none));

    /* 9.14 PUBLISH_NAMESPACE: Request ID 8, namespace [a, b], no parameters. */
    moq_bytes_t ab[] = { MOQ_BYTES_LITERAL("a"), MOQ_BYTES_LITERAL("b") };
    moq_namespace_t ns_ab = { ab, 2 };
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_publish_namespace(&w, 8, &ns_ab, &none), (int)MOQ_OK);
    static const uint8_t pn[] = { 0x06, 0x00, 0x07, 0x08, 0x02, 0x01, 'a', 0x01, 'b', 0x00 };
    check_bytes("PUBLISH_NAMESPACE", buf, moq_buf_writer_offset(&w), pn, sizeof(pn));
    moq_control_envelope_t env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_PUBLISH_NAMESPACE);
    moq_d21_publish_namespace_t pnd;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish_namespace(env.payload, env.payload_len, dparts, 40, &pnd),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(pnd.request_id, 8);
    MOQ_TEST_CHECK_EQ_SIZE(pnd.track_namespace.count, 2);

    /* 9.15 SUBSCRIBE_NAMESPACE: the prefix may be empty (0 to 32 fields). */
    moq_namespace_t root = { NULL, 0 };
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_subscribe_namespace(&w, 4, &root, &none), (int)MOQ_OK);
    static const uint8_t sn[] = { 0x50, 0x00, 0x03, 0x04, 0x00, 0x00 };
    check_bytes("SUBSCRIBE_NAMESPACE root", buf, moq_buf_writer_offset(&w), sn, sizeof(sn));
    env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_SUBSCRIBE_NAMESPACE);
    moq_d21_subscribe_namespace_t snd;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_subscribe_namespace(env.payload, env.payload_len, dparts, 40, &snd),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(snd.track_namespace_prefix.count, 0);

    /* 9.18 SUBSCRIBE_TRACKS takes subscription parameters (9.18.1): FORWARD 1 and
     * GROUP_ORDER 2 (which moved here from PUBLISH_OK). */
    moq_bytes_t a1[] = { MOQ_BYTES_LITERAL("a") };
    moq_namespace_t ns_a = { a1, 1 };
    moq_d21_msg_params_t p;
    memset(&p, 0, sizeof(p));
    p.has_forward = true; p.forward = 1;
    p.has_group_order = true; p.group_order = 2;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_subscribe_tracks(&w, 6, &ns_a, &p), (int)MOQ_OK);
    static const uint8_t st[] = { 0x51, 0x00, 0x09, 0x06, 0x01, 0x01, 'a',
                                  0x02, 0x10, 0x01, 0x12, 0x02 };
    check_bytes("SUBSCRIBE_TRACKS", buf, moq_buf_writer_offset(&w), st, sizeof(st));
    env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_SUBSCRIBE_TRACKS);
    moq_d21_subscribe_tracks_t std;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_subscribe_tracks(env.payload, env.payload_len, dparts, 40, &std),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(std.params.has_forward && std.params.forward == 1);
    MOQ_TEST_CHECK(std.params.has_group_order && std.params.group_order == 2);

    /* Namespace requests take only an AUTHORIZATION_TOKEN; anything else is a
     * violation on receipt and refused on send. */
    static const uint8_t pn_fwd[] = { 0x08, 0x01, 0x01, 'a', 0x01, 0x10, 0x01 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish_namespace(pn_fwd, sizeof(pn_fwd), dparts, 40, &pnd),
                          (int)MOQ_ERR_PROTO);
    static const uint8_t sn_fwd[] = { 0x04, 0x00, 0x01, 0x10, 0x01 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_subscribe_namespace(sn_fwd, sizeof(sn_fwd), dparts, 40, &snd),
                          (int)MOQ_ERR_PROTO);
    memset(&p, 0, sizeof(p));
    p.has_forward = true;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_subscribe_namespace(&w, 4, &root, &p), (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_publish_namespace(&w, 4, &ns_a, &p), (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);

    /* 9.16 / 9.17 NAMESPACE and NAMESPACE_DONE carry only a suffix. */
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_namespace_msg(&w, &ns_a, false), (int)MOQ_OK);
    static const uint8_t nm[] = { 0x08, 0x00, 0x03, 0x01, 0x01, 'a' };
    check_bytes("NAMESPACE", buf, moq_buf_writer_offset(&w), nm, sizeof(nm));
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_namespace_msg(&w, &ns_a, true), (int)MOQ_OK);
    static const uint8_t nd[] = { 0x0E, 0x00, 0x03, 0x01, 0x01, 'a' };
    check_bytes("NAMESPACE_DONE", buf, moq_buf_writer_offset(&w), nd, sizeof(nd));
    env = decode_env(buf, moq_buf_writer_offset(&w), MOQ_D21_NAMESPACE_DONE);
    moq_namespace_t suffix;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_namespace_msg(env.payload, env.payload_len, dparts, 40, &suffix),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(suffix.count, 1);
    /* A zero-field suffix is the prefix itself (4.2). */
    static const uint8_t rootsuf[] = { 0x00 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_namespace_msg(rootsuf, sizeof(rootsuf), dparts, 40, &suffix),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(suffix.count, 0);

    /* Namespace limits hold in every message that carries one (8.7): 33 fields
     * and an empty field are violations. */
    uint8_t many[160];
    size_t n = 0;
    many[n++] = 0x04; many[n++] = 33;
    for (int i = 0; i < 33; i++) { many[n++] = 0x01; many[n++] = 'x'; }
    many[n++] = 0x00;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_subscribe_namespace(many, n, dparts, 40, &snd), (int)MOQ_ERR_PROTO);
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_subscribe_tracks(many, n, dparts, 40, &std), (int)MOQ_ERR_PROTO);
    n = 0; many[n++] = 33;
    for (int i = 0; i < 33; i++) { many[n++] = 0x01; many[n++] = 'x'; }
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_namespace_msg(many, n, dparts, 40, &suffix), (int)MOQ_ERR_PROTO);
    static const uint8_t empty_field[] = { 0x01, 0x00 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_namespace_msg(empty_field, sizeof(empty_field), dparts, 40, &suffix),
                          (int)MOQ_ERR_PROTO);
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
    t_subscribe();
    t_subscribe_params();
    t_namespace_limits();
    t_subscribe_ok();
    t_request_update();
    t_request_ok();
    t_request_error();
    t_code_registries();
    t_publish();
    t_publish_done();
    t_publish_state_notify();
    t_publish_skipped();
    t_fetch();
    t_fetch_rejects_draft18_forms();
    t_fetch_ok();
    t_fetch_header();
    t_goaway();
    t_track_status();
    t_namespace_family();

    if (failures)
        fprintf(stderr, "test_control_d21: %d byte-vector failures\n", failures);
    MOQ_TEST_PASS("test_control_d21");
}
