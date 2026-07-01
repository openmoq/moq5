#include <moq/buf.h>
#include <moq/wire.h>
#include "test_support.h"
#include <string.h>

int main(void)
{
    int failures = 0;

    /* == Reader tests ============================================== */

    /* -- Varint read ------------------------------------------------ */
    {
        uint8_t buf[8];
        size_t n = moq_quic_varint_encode(12345, buf, sizeof(buf));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, n);

        uint64_t v = 0;
        MOQ_TEST_CHECK(moq_buf_read_varint(&r, &v) == MOQ_OK);
        MOQ_TEST_CHECK(v == 12345);
        MOQ_TEST_CHECK(moq_buf_reader_remaining(&r) == 0);
    }

    /* -- uint16 read ------------------------------------------------ */
    {
        uint8_t buf[] = { 0x01, 0x00 };
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, 2);

        uint16_t v = 0;
        MOQ_TEST_CHECK(moq_buf_read_uint16(&r, &v) == MOQ_OK);
        MOQ_TEST_CHECK(v == 256);
        MOQ_TEST_CHECK(moq_buf_reader_remaining(&r) == 0);
    }

    /* -- Span read -------------------------------------------------- */
    {
        /* varint(5) + "hello" */
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_buf_write_span(&w, MOQ_BYTES_LITERAL("hello"));

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_bytes_t span = {0};
        MOQ_TEST_CHECK(moq_buf_read_span(&r, &span) == MOQ_OK);
        MOQ_TEST_CHECK(span.len == 5);
        MOQ_TEST_CHECK(memcmp(span.data, "hello", 5) == 0);
        MOQ_TEST_CHECK(moq_buf_reader_remaining(&r) == 0);
    }

    /* -- Raw read --------------------------------------------------- */
    {
        uint8_t buf[] = { 0xAA, 0xBB, 0xCC };
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, 3);

        moq_bytes_t out = {0};
        MOQ_TEST_CHECK(moq_buf_read_raw(&r, 2, &out) == MOQ_OK);
        MOQ_TEST_CHECK(out.len == 2);
        MOQ_TEST_CHECK(out.data[0] == 0xAA && out.data[1] == 0xBB);
        MOQ_TEST_CHECK(moq_buf_reader_remaining(&r) == 1);
    }

    /* -- Skip ------------------------------------------------------- */
    {
        uint8_t buf[4] = {0};
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, 4);
        MOQ_TEST_CHECK(moq_buf_skip(&r, 3) == MOQ_OK);
        MOQ_TEST_CHECK(moq_buf_reader_remaining(&r) == 1);
    }

    /* -- Reader truncation does not advance cursor ------------------ */
    {
        uint8_t buf[1] = { 0x40 }; /* 2-byte varint header, only 1 byte */
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, 1);

        uint64_t v = 0;
        MOQ_TEST_CHECK(moq_buf_read_varint(&r, &v) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_reader_offset(&r) == 0);

        uint16_t u = 0;
        MOQ_TEST_CHECK(moq_buf_read_uint16(&r, &u) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_reader_offset(&r) == 0);

        MOQ_TEST_CHECK(moq_buf_skip(&r, 5) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_reader_offset(&r) == 0);

        moq_bytes_t s = {0};
        MOQ_TEST_CHECK(moq_buf_read_raw(&r, 2, &s) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_reader_offset(&r) == 0);
    }

    /* -- Span read truncation: length ok but data short ------------- */
    {
        uint8_t buf[] = { 0x05, 0xAA }; /* length=5 but only 1 data byte */
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, 2);
        moq_bytes_t s = {0};
        MOQ_TEST_CHECK(moq_buf_read_span(&r, &s) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_reader_offset(&r) == 0);
    }

    /* == Writer tests ============================================== */

    /* -- Varint write ----------------------------------------------- */
    {
        uint8_t buf[8];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_buf_write_varint(&w, 42) == MOQ_OK);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 1);
        MOQ_TEST_CHECK(buf[0] == 42);
    }

    /* -- uint16 write ----------------------------------------------- */
    {
        uint8_t buf[4];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_buf_write_uint16(&w, 0x1234) == MOQ_OK);
        MOQ_TEST_CHECK(buf[0] == 0x12 && buf[1] == 0x34);
    }

    /* -- Span write ------------------------------------------------- */
    {
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_buf_write_span(&w, MOQ_BYTES_LITERAL("abc")) == MOQ_OK);
        /* varint(3) + "abc" = 4 bytes */
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 4);
    }

    /* -- Raw write -------------------------------------------------- */
    {
        uint8_t buf[4];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_buf_write_raw(&w, (const uint8_t *)"XY", 2) == MOQ_OK);
        MOQ_TEST_CHECK(buf[0] == 'X' && buf[1] == 'Y');
    }

    /* -- Writer capacity failure does not advance cursor ------------ */
    {
        uint8_t buf[1];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, 1);

        MOQ_TEST_CHECK(moq_buf_write_uint16(&w, 0) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 0);

        MOQ_TEST_CHECK(moq_buf_write_raw(&w, (const uint8_t *)"AB", 2) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 0);

        MOQ_TEST_CHECK(moq_buf_write_span(&w, MOQ_BYTES_LITERAL("too long")) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 0);
    }

    /* -- Writer rejects invalid varint value ------------------------- */
    {
        uint8_t buf[8];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_buf_write_varint(&w, MOQ_QUIC_VARINT_MAX + 1) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 0);
    }

    /* -- Reserve/patch uint16 --------------------------------------- */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        size_t len_offset = 0;
        MOQ_TEST_CHECK(moq_buf_reserve_uint16(&w, &len_offset) == MOQ_OK);
        MOQ_TEST_CHECK(len_offset == 0);

        size_t payload_start = moq_buf_writer_offset(&w);
        MOQ_TEST_CHECK(moq_buf_write_raw(&w, (const uint8_t *)"payload", 7) == MOQ_OK);

        uint16_t payload_len = (uint16_t)(moq_buf_writer_offset(&w) - payload_start);
        MOQ_TEST_CHECK(moq_buf_patch_uint16(&w, len_offset, payload_len) == MOQ_OK);

        MOQ_TEST_CHECK(buf[0] == 0x00 && buf[1] == 0x07);
    }

    /* == Namespace tests ============================================ */

    /* -- Valid 2-field namespace roundtrip --------------------------- */
    {
        moq_bytes_t parts[] = {
            MOQ_BYTES_LITERAL("chat.example.com"),
            MOQ_BYTES_LITERAL("room42"),
        };
        moq_namespace_t ns = { .parts = parts, .count = 2 };

        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_buf_write_namespace(&w, &ns) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_bytes_t dec_parts[32];
        moq_namespace_t dec_ns;
        MOQ_TEST_CHECK(moq_buf_read_namespace(&r, dec_parts, 32, &dec_ns) == MOQ_OK);
        MOQ_TEST_CHECK(dec_ns.count == 2);
        MOQ_TEST_CHECK(dec_ns.parts[0].len == 16);
        MOQ_TEST_CHECK(memcmp(dec_ns.parts[0].data, "chat.example.com", 16) == 0);
        MOQ_TEST_CHECK(dec_ns.parts[1].len == 6);
    }

    /* -- Count 0 is invalid for general namespace ------------------- */
    {
        uint8_t buf[] = { 0x00 }; /* count = 0 */
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, 1);
        moq_bytes_t parts[32];
        moq_namespace_t ns;
        MOQ_TEST_CHECK(moq_buf_read_namespace(&r, parts, 32, &ns) == MOQ_ERR_PROTO);
        MOQ_TEST_CHECK(moq_buf_reader_offset(&r) == 0);
    }

    /* -- Count > 32 is invalid -------------------------------------- */
    {
        uint8_t buf[2];
        moq_quic_varint_encode(33, buf, 2);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, 1);
        moq_bytes_t parts[64];
        moq_namespace_t ns;
        MOQ_TEST_CHECK(moq_buf_read_namespace(&r, parts, 64, &ns) == MOQ_ERR_PROTO);
        MOQ_TEST_CHECK(moq_buf_reader_offset(&r) == 0);
    }

    /* -- Empty field (length 0) is invalid -------------------------- */
    {
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_buf_write_varint(&w, 1);   /* count = 1 */
        moq_buf_write_varint(&w, 0);   /* field length = 0 */

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_bytes_t parts[32];
        moq_namespace_t ns;
        MOQ_TEST_CHECK(moq_buf_read_namespace(&r, parts, 32, &ns) == MOQ_ERR_PROTO);
        MOQ_TEST_CHECK(moq_buf_reader_offset(&r) == 0);
    }

    /* -- Write rejects count 0 ------------------------------------- */
    {
        moq_namespace_t ns = { .parts = NULL, .count = 0 };
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_buf_write_namespace(&w, &ns) == MOQ_ERR_INVAL);
    }

    /* -- Write rejects count > 32 ---------------------------------- */
    {
        moq_bytes_t parts[33];
        for (int i = 0; i < 33; i++) parts[i] = MOQ_BYTES_LITERAL("x");
        moq_namespace_t ns = { .parts = parts, .count = 33 };
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_buf_write_namespace(&w, &ns) == MOQ_ERR_INVAL);
    }

    /* -- Write rejects empty field ---------------------------------- */
    {
        moq_bytes_t parts[] = { { .data = NULL, .len = 0 } };
        moq_namespace_t ns = { .parts = parts, .count = 1 };
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_buf_write_namespace(&w, &ns) == MOQ_ERR_INVAL);
    }

    /* -- Namespace total > 4096 rejected ----------------------------- */
    {
        /* Build a namespace with fields that sum > 4096. */
        char big_field[4097];
        memset(big_field, 'A', sizeof(big_field));
        moq_bytes_t parts[] = {
            { .data = (const uint8_t *)big_field, .len = 4097 }
        };
        moq_namespace_t ns = { .parts = parts, .count = 1 };
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_buf_write_namespace(&w, &ns) == MOQ_ERR_INVAL);
    }

    /* -- Namespace read truncation restores cursor ------------------- */
    {
        /* count=2 but only one field encoded. */
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_buf_write_varint(&w, 2);               /* count = 2 */
        moq_buf_write_span(&w, MOQ_BYTES_LITERAL("field1")); /* only 1 field */

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_bytes_t dec_parts[32];
        moq_namespace_t dec_ns;
        MOQ_TEST_CHECK(moq_buf_read_namespace(&r, dec_parts, 32, &dec_ns) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_reader_offset(&r) == 0);
    }

    /* -- NULL arg guards ---------------------------------------------- */
    {
        uint64_t v = 0;
        uint16_t u = 0;
        moq_bytes_t b = {0};
        size_t off = 0;
        moq_namespace_t ns = {0};

        MOQ_TEST_CHECK(moq_buf_read_varint(NULL, &v) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_read_uint16(NULL, &u) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_read_span(NULL, &b) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_read_raw(NULL, 0, &b) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_skip(NULL, 0) == MOQ_ERR_INVAL);

        MOQ_TEST_CHECK(moq_buf_write_varint(NULL, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_write_uint16(NULL, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_write_raw(NULL, NULL, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_reserve_uint16(NULL, &off) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_patch_uint16(NULL, 0, 0) == MOQ_ERR_INVAL);

        MOQ_TEST_CHECK(moq_buf_read_namespace(NULL, NULL, 0, &ns) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_write_namespace(NULL, &ns) == MOQ_ERR_INVAL);

        /* Out-param NULL */
        uint8_t dummy[4] = {0x01, 0x01, 0x00, 0x00};
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, dummy, 4);
        MOQ_TEST_CHECK(moq_buf_read_varint(&r, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_read_uint16(&r, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_read_span(&r, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_read_raw(&r, 1, NULL) == MOQ_ERR_INVAL);

        moq_buf_writer_t wr;
        moq_buf_writer_init(&wr, dummy, 4);
        MOQ_TEST_CHECK(moq_buf_reserve_uint16(&wr, NULL) == MOQ_ERR_INVAL);

        /* write_raw with NULL data and len > 0 */
        MOQ_TEST_CHECK(moq_buf_write_raw(&wr, NULL, 5) == MOQ_ERR_INVAL);

        /* write_span with NULL data and len > 0 */
        moq_bytes_t bad_span = { .data = NULL, .len = 3 };
        MOQ_TEST_CHECK(moq_buf_write_span(&wr, bad_span) == MOQ_ERR_INVAL);
    }

    /* -- Zero-length NULL buffer: ptr helpers return NULL ------------ */
    {
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, NULL, 0);
        MOQ_TEST_CHECK(moq_buf_reader_ptr(&r) == NULL);
        MOQ_TEST_CHECK(moq_buf_reader_remaining(&r) == 0);
        {
            uint64_t v = 0;
            MOQ_TEST_CHECK(moq_buf_read_varint(&r, &v) == MOQ_ERR_BUFFER);
        }

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, NULL, 0);
        MOQ_TEST_CHECK(moq_buf_writer_ptr(&w) == NULL);
        MOQ_TEST_CHECK(moq_buf_writer_remaining(&w) == 0);
        MOQ_TEST_CHECK(moq_buf_write_varint(&w, 0) == MOQ_ERR_BUFFER);
    }

    /* -- Corrupted pos past len/cap: helpers don't underflow --------- */
    {
        uint64_t v = 0;
        uint16_t u = 0;
        moq_bytes_t b = {0};
        size_t off = 0;

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, (const uint8_t *)"abc", 3);
        r.pos = 100; /* corrupt */
        MOQ_TEST_CHECK(moq_buf_reader_remaining(&r) == 0);
        MOQ_TEST_CHECK(moq_buf_reader_ptr(&r) == NULL);
        MOQ_TEST_CHECK(moq_buf_reader_offset(&r) == 100);
        MOQ_TEST_CHECK(moq_buf_read_varint(&r, &v) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_read_uint16(&r, &u) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_read_raw(&r, 0, &b) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_skip(&r, 0) == MOQ_ERR_INVAL);

        moq_buf_writer_t w;
        uint8_t wbuf[4];
        moq_buf_writer_init(&w, wbuf, 4);
        w.pos = 99; /* corrupt */
        MOQ_TEST_CHECK(moq_buf_writer_remaining(&w) == 0);
        MOQ_TEST_CHECK(moq_buf_writer_ptr(&w) == NULL);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 99);
        MOQ_TEST_CHECK(moq_buf_write_varint(&w, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_write_uint16(&w, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_write_raw(&w, NULL, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_reserve_uint16(&w, &off) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_patch_uint16(&w, 0, 0) == MOQ_ERR_INVAL);
    }

    /* -- Non-zero length/cap with NULL base is invalid ---------------- */
    {
        uint64_t v = 0;
        moq_bytes_t b = {0};
        moq_buf_reader_t r = { NULL, 4, 0 };
        MOQ_TEST_CHECK(moq_buf_read_varint(&r, &v) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_read_raw(&r, 0, &b) == MOQ_ERR_INVAL);

        moq_buf_writer_t w = { NULL, 4, 0 };
        MOQ_TEST_CHECK(moq_buf_write_varint(&w, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_write_raw(&w, NULL, 0) == MOQ_ERR_INVAL);
    }

    /* -- NULL struct pointer: helpers return safe defaults ----------- */
    {
        MOQ_TEST_CHECK(moq_buf_reader_remaining(NULL) == 0);
        MOQ_TEST_CHECK(moq_buf_reader_offset(NULL) == 0);
        MOQ_TEST_CHECK(moq_buf_reader_ptr(NULL) == NULL);

        MOQ_TEST_CHECK(moq_buf_writer_remaining(NULL) == 0);
        MOQ_TEST_CHECK(moq_buf_writer_offset(NULL) == 0);
        MOQ_TEST_CHECK(moq_buf_writer_ptr(NULL) == NULL);
    }

    /* == Namespace prefix tests ======================================= */

    /* -- Prefix count 0 roundtrip ----------------------------------- */
    {
        moq_namespace_t ns = { .parts = NULL, .count = 0 };
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_buf_write_namespace_prefix(&w, &ns) == MOQ_OK);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 1);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_bytes_t dec_parts[8];
        moq_namespace_t dec_ns;
        MOQ_TEST_CHECK(moq_buf_read_namespace_prefix(&r, dec_parts, 8, &dec_ns) == MOQ_OK);
        MOQ_TEST_CHECK(dec_ns.count == 0);
    }

    /* -- Prefix count 0 can decode without a parts array ------------ */
    {
        uint8_t buf[] = { 0x00 };
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, sizeof(buf));
        moq_namespace_t ns = {0};
        MOQ_TEST_CHECK(moq_buf_read_namespace_prefix(&r, NULL, 0, &ns) == MOQ_OK);
        MOQ_TEST_CHECK(ns.parts == NULL);
        MOQ_TEST_CHECK(ns.count == 0);
        MOQ_TEST_CHECK(moq_buf_reader_remaining(&r) == 0);
    }

    /* -- Prefix with fields roundtrip ------------------------------- */
    {
        moq_bytes_t parts[] = {
            MOQ_BYTES_LITERAL("a"), MOQ_BYTES_LITERAL("bb"), MOQ_BYTES_LITERAL("ccc"),
        };
        moq_namespace_t ns = { .parts = parts, .count = 3 };
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_buf_write_namespace_prefix(&w, &ns) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_bytes_t dec_parts[8];
        moq_namespace_t dec_ns;
        MOQ_TEST_CHECK(moq_buf_read_namespace_prefix(&r, dec_parts, 8, &dec_ns) == MOQ_OK);
        MOQ_TEST_CHECK(dec_ns.count == 3);
        MOQ_TEST_CHECK(dec_ns.parts[2].len == 3);
    }

    /* -- Prefix count > 32 rejected --------------------------------- */
    {
        uint8_t buf[2];
        moq_quic_varint_encode(33, buf, 2);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, 1);
        moq_bytes_t parts[64];
        moq_namespace_t ns;
        MOQ_TEST_CHECK(moq_buf_read_namespace_prefix(&r, parts, 64, &ns) == MOQ_ERR_PROTO);
        MOQ_TEST_CHECK(moq_buf_reader_offset(&r) == 0);
    }

    /* -- Prefix empty field rejected -------------------------------- */
    {
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_buf_write_varint(&w, 1);
        moq_buf_write_varint(&w, 0);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_bytes_t parts[8];
        moq_namespace_t ns;
        MOQ_TEST_CHECK(moq_buf_read_namespace_prefix(&r, parts, 8, &ns) == MOQ_ERR_PROTO);
        MOQ_TEST_CHECK(moq_buf_reader_offset(&r) == 0);
    }

    /* -- General namespace still rejects count 0 -------------------- */
    {
        uint8_t buf[] = { 0x00 };
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, 1);
        moq_bytes_t parts[8];
        moq_namespace_t ns;
        MOQ_TEST_CHECK(moq_buf_read_namespace(&r, parts, 8, &ns) == MOQ_ERR_PROTO);
    }

    /* -- Prefix truncation restores cursor -------------------------- */
    {
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_buf_write_varint(&w, 2);
        moq_buf_write_span(&w, MOQ_BYTES_LITERAL("one"));

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_bytes_t parts[8];
        moq_namespace_t ns;
        MOQ_TEST_CHECK(moq_buf_read_namespace_prefix(&r, parts, 8, &ns) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_reader_offset(&r) == 0);
    }

    /* -- Prefix read with fields requires a parts array ------------- */
    {
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_buf_write_varint(&w, 1);
        moq_buf_write_span(&w, MOQ_BYTES_LITERAL("one"));

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_namespace_t ns;
        MOQ_TEST_CHECK(moq_buf_read_namespace_prefix(&r, NULL, 1, &ns) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_reader_offset(&r) == 0);
    }

    /* -- Prefix write rejects count > 32 ---------------------------- */
    {
        moq_bytes_t parts[33];
        for (int i = 0; i < 33; i++) parts[i] = MOQ_BYTES_LITERAL("x");
        moq_namespace_t ns = { .parts = parts, .count = 33 };
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_buf_write_namespace_prefix(&w, &ns) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 0);
    }

    /* -- Prefix write rejects empty fields -------------------------- */
    {
        moq_bytes_t parts[] = { { .data = NULL, .len = 0 } };
        moq_namespace_t ns = { .parts = parts, .count = 1 };
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_buf_write_namespace_prefix(&w, &ns) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 0);
    }

    MOQ_TEST_PASS("test_buf");
    return failures;
}
