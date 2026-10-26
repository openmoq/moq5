/*
 * Draft-18 conformance closeout: SETUP Options (§10.3.1), defined-but-unmodeled
 * message parameters (§10.2.5/6/13/14), and the descending-FETCH capability guard.
 *
 * Setup Options are vi64 Key-Value-Pairs — NOT the draft-16 QUIC-varint KVP form.
 * The two integer encodings diverge in the value range 64..127 (vi64: one byte;
 * QUIC varint: two), so the boundary tests below pin the vi64 form explicitly.
 */
#include <moq/moq.h>
#include <moq/sim.h>
#include <moq/control_d18.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"
#include <stdlib.h>

/* The v0 record ends at `authority`, with struct_size first. */
_Static_assert(MOQ_D18_SETUP_OPTS_V0_SIZE ==
    offsetof(moq_d18_setup_opts_t, authority) + sizeof(moq_bytes_t), "v0 end");
_Static_assert(offsetof(moq_d18_setup_opts_t, struct_size) == 0, "size leads");
#if UINTPTR_MAX == UINT64_MAX
_Static_assert(MOQ_D18_SETUP_OPTS_V0_SIZE == 696, "LP64 v0 floor");
#endif

static int failures = 0;

/* -- Session harness ------------------------------------------------ */

static moq_session_t *make_started(moq_perspective_t persp, uint64_t cache_size)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = MOQ_VERSION_DRAFT_18;
    if (cache_size > 0) {
        cfg.send_auth_token_cache_size = true;
        cfg.auth_token_cache_size = cache_size;
    }
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (moq_session_start(s, 0) < 0) { moq_session_destroy(s); return NULL; }
    return s;
}

/* Feed a hand-built SETUP control message carrying `payload`. */
static moq_result_t feed_setup(moq_session_t *s, const uint8_t *payload,
                               size_t plen)
{
    uint8_t msg[160];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_buf_write_vi64(&w, 0x2F00);                  /* SETUP type */
    moq_buf_write_uint16(&w, (uint16_t)plen);
    if (plen > 0) moq_buf_write_raw(&w, payload, plen);
    return moq_session_on_control_bytes(s, msg, moq_buf_writer_offset(&w), 0);
}

/* -- Param-block builders -------------------------------------------- */

static void put_vi64(moq_buf_writer_t *w, uint64_t v) { moq_buf_write_vi64(w, v); }

static void put_ns1(moq_buf_writer_t *w, const char *part)
{
    put_vi64(w, 1);
    put_vi64(w, strlen(part));
    moq_buf_write_raw(w, (const uint8_t *)part, strlen(part));
}

static void put_span(moq_buf_writer_t *w, const char *b)
{
    put_vi64(w, strlen(b));
    moq_buf_write_raw(w, (const uint8_t *)b, strlen(b));
}

int main(void)
{
    /* == 1. SETUP option codec: vi64 KVP form ========================== */

    /* 1a. Encode: MAX_AUTH_TOKEN_CACHE_SIZE 100 emits the vi64 single-byte
     * value (0x64) at the divergence boundary — a QUIC-varint encoder would
     * emit two bytes (0x40 0x64). */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d18_setup_opts_t o;
        moq_d18_setup_opts_init(&o);
        o.has_max_auth_token_cache_size = true;
        o.max_auth_token_cache_size = 100;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup_opts(&w, &o, sizeof(o)), (int)MOQ_OK);
        size_t n = moq_buf_writer_offset(&w);
        MOQ_TEST_CHECK(n >= 4);
        /* 16-bit Length is the 2 bytes before the payload; payload is exactly
         * {delta 0x04, vi64 value 0x64}. */
        MOQ_TEST_CHECK_EQ_HEX(buf[n - 2], 0x04);
        MOQ_TEST_CHECK_EQ_HEX(buf[n - 1], 0x64);
        MOQ_TEST_CHECK_EQ_HEX(buf[n - 3], 0x02);   /* Length low byte == 2 */
        MOQ_TEST_CHECK_EQ_HEX(buf[n - 4], 0x00);   /* Length high byte */
    }

    /* 1b. Decode the vi64 form back: {0x04, 0x64} -> 100. */
    {
        uint8_t p[] = { 0x04, 0x64 };
        moq_d18_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_setup_opts(p, sizeof(p), &o, sizeof(o)),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(o.has_max_auth_token_cache_size);
        MOQ_TEST_CHECK_EQ_U64(o.max_auth_token_cache_size, 100);
    }

    /* 1c. QUIC-varint-shaped bytes for 100 ({0x40,0x64}) must NOT decode as
     * 100 under the vi64 walker (it reads 0x40 as the one-byte value 64). */
    {
        uint8_t p[] = { 0x04, 0x40, 0x64 };
        moq_d18_setup_opts_t o;
        moq_result_t rc = moq_d18_decode_setup_opts(p, sizeof(p), &o, sizeof(o));
        MOQ_TEST_CHECK(rc < 0 ||
                       !(o.has_max_auth_token_cache_size &&
                         o.max_auth_token_cache_size == 100));
    }

    /* 1d. Full walk: PATH + MAX + AUTHORITY + unknown even + unknown odd
     * (duplicated) are all consumed; unknowns ignored, flags recorded. */
    {
        uint8_t p[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0x01); put_vi64(&w, 2);            /* PATH "ab" */
        moq_buf_write_raw(&w, (const uint8_t *)"ab", 2);
        put_vi64(&w, 0x03); put_vi64(&w, 100);          /* delta 3 -> 0x04 MAX */
        put_vi64(&w, 0x01); put_vi64(&w, 1);            /* delta 1 -> 0x05 AUTHORITY */
        moq_buf_write_raw(&w, (const uint8_t *)"x", 1);
        put_vi64(&w, 0x01); put_vi64(&w, 5);            /* delta 1 -> 0x06 unknown even */
        put_vi64(&w, 0x01); put_vi64(&w, 3);            /* delta 1 -> 0x07 unknown odd */
        moq_buf_write_raw(&w, (const uint8_t *)"zzz", 3);
        put_vi64(&w, 0x00); put_vi64(&w, 1);            /* delta 0 -> dup unknown odd */
        moq_buf_write_raw(&w, (const uint8_t *)"q", 1);
        moq_d18_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_setup_opts(p, moq_buf_writer_offset(&w), &o, sizeof(o)),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(o.has_path && o.has_authority);
        MOQ_TEST_CHECK_EQ_U64(o.max_auth_token_cache_size, 100);
        MOQ_TEST_CHECK(o.path.len == 2 && memcmp(o.path.data, "ab", 2) == 0);
        MOQ_TEST_CHECK(o.authority.len == 1 && o.authority.data[0] == 'x');
        /* Decoded values borrow from the payload. */
        MOQ_TEST_CHECK(o.path.data == p + 2 && o.authority.data == p + 8);
    }

    /* 1d'. Encode PATH and AUTHORITY (§10.3.1.1, §10.3.1.2): the moqt URI
     * moqt://127.0.0.1:31000/moq?interop=1 gives PATH "/moq?interop=1" (the
     * path-abempty, "?" and the query) and AUTHORITY "127.0.0.1:31000". Type
     * 0x2F00 is the two-byte vi64 af 00; PATH is the first option (delta
     * 0x01), AUTHORITY follows (delta 0x05 - 0x01 = 0x04); each odd type
     * carries a vi64 Length; the payload is 2 + 14 + 2 + 15 = 33 bytes. */
    {
        static const uint8_t want[] = {
            0xaf, 0x00, 0x00, 0x21,
            0x01, 0x0e, '/','m','o','q','?','i','n','t','e','r','o','p','=','1',
            0x04, 0x0f, '1','2','7','.','0','.','0','.','1',':','3','1','0','0','0',
        };
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d18_setup_opts_t o;
        moq_d18_setup_opts_init(&o);
        o.has_path = true;
        o.path = (moq_bytes_t){ (const uint8_t *)"/moq?interop=1", 14 };
        o.has_authority = true;
        o.authority = (moq_bytes_t){ (const uint8_t *)"127.0.0.1:31000", 15 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup_opts(&w, &o, sizeof(o)), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), sizeof(want));
        MOQ_TEST_CHECK(memcmp(buf, want, sizeof(want)) == 0);
        moq_d18_setup_opts_t d;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_setup_opts(want + 4,
                              sizeof(want) - 4, &d, sizeof(d)), (int)MOQ_OK);
        MOQ_TEST_CHECK(d.has_path && d.path.len == 14 &&
                       memcmp(d.path.data, "/moq?interop=1", 14) == 0);
        MOQ_TEST_CHECK(d.has_authority && d.authority.len == 15 &&
                       memcmp(d.authority.data, "127.0.0.1:31000", 15) == 0);
    }

    /* 1d''. Every option this side sends, in ascending type order with each
     * type a delta from the previous one: PATH "/p" (delta 1), a USE_VALUE
     * token of type 7 and value "t" (0x03, delta 2; Alias Type 3, Token Type
     * 7, value), MAX_AUTH_TOKEN_CACHE_SIZE 100 (0x04, delta 1, vi64 0x64),
     * AUTHORITY "h" (0x05, delta 1). An explicitly empty PATH is a present
     * option with a zero Length. */
    {
        static const uint8_t want[] = {
            0xaf, 0x00, 0x00, 0x0e,
            0x01, 0x02, '/', 'p',
            0x02, 0x03, 0x03, 0x07, 't',
            0x01, 0x64,
            0x01, 0x01, 'h',
        };
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d18_setup_opts_t o;
        moq_d18_setup_opts_init(&o);
        o.has_path = true;
        o.path = (moq_bytes_t){ (const uint8_t *)"/p", 2 };
        o.auth_token_count = 1;
        o.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        o.auth_tokens[0].token_type = 7;
        o.auth_tokens[0].token_value = (moq_bytes_t){ (const uint8_t *)"t", 1 };
        o.has_max_auth_token_cache_size = true;
        o.max_auth_token_cache_size = 100;
        o.has_authority = true;
        o.authority = (moq_bytes_t){ (const uint8_t *)"h", 1 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup_opts(&w, &o, sizeof(o)), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), sizeof(want));
        MOQ_TEST_CHECK(memcmp(buf, want, sizeof(want)) == 0);

        static const uint8_t empty_path[] = {
            0xaf, 0x00, 0x00, 0x05, 0x01, 0x00, 0x04, 0x01, 'h',
        };
        o.path = (moq_bytes_t){ NULL, 0 };
        o.auth_token_count = 0;
        o.has_max_auth_token_cache_size = false;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup_opts(&w, &o, sizeof(o)), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), sizeof(empty_path));
        MOQ_TEST_CHECK(memcmp(buf, empty_path, sizeof(empty_path)) == 0);

        /* A value with a length but no bytes is refused before anything is
         * written; so is a writer too short for the whole message. */
        o.authority = (moq_bytes_t){ NULL, 1 };
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup_opts(&w, &o, sizeof(o)), (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
        o.authority = (moq_bytes_t){ (const uint8_t *)"h", 1 };
        moq_buf_writer_init(&w, buf, sizeof(empty_path) - 1);
        MOQ_TEST_CHECK(moq_d18_encode_setup_opts(&w, &o, sizeof(o)) < 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
    }

    /* 1d-sized. The sized record at the codec boundary. A payload carrying
     * every option with a value is decoded into allocations of exactly the
     * caller's size: nothing is written past it, struct_size says what was
     * written, a failure leaves a cleared record, and the encoder reads
     * nothing past the caller's struct_size. Heap allocations of the exact
     * size let ASan see any overrun. */
    {
        static const uint8_t full[] = {
            0x01, 0x02, '/', 'p',                    /* PATH "/p" */
            0x02, 0x03, 0x03, 0x07, 't',             /* token USE_VALUE 7 "t" */
            0x01, 0x64,                              /* cache size 100 */
            0x01, 0x01, 'h',                         /* AUTHORITY "h" */
        };
        const size_t v0 = MOQ_D18_SETUP_OPTS_V0_SIZE;
        uint8_t *raw;

        /* Below the floor: refused, nothing written. */
        raw = malloc(v0);
        MOQ_TEST_CHECK(raw != NULL);
        if (raw) {
            memset(raw, 0xa5, v0);
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_setup_opts(full, sizeof(full),
                (moq_d18_setup_opts_t *)(void *)raw, v0 - 1), (int)MOQ_ERR_INVAL);
            bool untouched = true;
            for (size_t i = 0; i < v0; i++) untouched &= raw[i] == 0xa5;
            MOQ_TEST_CHECK(untouched);
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_setup_opts(full, sizeof(full),
                NULL, v0), (int)MOQ_ERR_INVAL);
            free(raw);
        }

        /* Exactly the v0 prefix, in an allocation of that size. */
        raw = malloc(v0);
        MOQ_TEST_CHECK(raw != NULL);
        if (raw) {
            moq_d18_setup_opts_t *o = (moq_d18_setup_opts_t *)(void *)raw;
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_setup_opts(full, sizeof(full),
                o, v0), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(o->struct_size, v0);
            MOQ_TEST_CHECK(o->has_path && o->path.len == 2 &&
                           memcmp(o->path.data, "/p", 2) == 0);
            MOQ_TEST_CHECK(o->has_authority && o->authority.len == 1 &&
                           o->authority.data[0] == 'h');
            MOQ_TEST_CHECK_EQ_SIZE(o->auth_token_count, 1);
            MOQ_TEST_CHECK_EQ_U64(o->max_auth_token_cache_size, 100);
            /* The same record re-encodes to the same bytes through the
             * public encoder, reading only its v0 size. */
            uint8_t buf[64];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup_opts(&w, o, v0), (int)MOQ_OK);
            MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 4 + sizeof(full) &&
                           buf[2] == 0 && buf[3] == sizeof(full) &&
                           memcmp(buf + 4, full, sizeof(full)) == 0);
            free(raw);
        }

        /* A caller from a future, larger layout: exactly the fields this
         * library knows are written, the size stamped, the rest untouched. */
        const size_t future = sizeof(moq_d18_setup_opts_t) + 32;
        raw = malloc(future);
        MOQ_TEST_CHECK(raw != NULL);
        if (raw) {
            memset(raw, 0x5a, future);
            moq_d18_setup_opts_t *o = (moq_d18_setup_opts_t *)(void *)raw;
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_setup_opts(full, sizeof(full),
                o, future), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(o->struct_size, sizeof(moq_d18_setup_opts_t));
            bool tail = true;
            for (size_t i = sizeof(moq_d18_setup_opts_t); i < future; i++)
                tail &= raw[i] == 0x5a;
            MOQ_TEST_CHECK(tail);
            /* Encoding from it: a struct_size within the allocation is
             * fine; one claiming more than the allocation, or less than v0,
             * is refused with the writer untouched. */
            uint8_t buf[64];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            o->struct_size = (uint32_t)future;
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup_opts(&w, o, future), (int)MOQ_OK);
            MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 4 + sizeof(full) &&
                           memcmp(buf + 4, full, sizeof(full)) == 0);
            moq_buf_writer_init(&w, buf, sizeof(buf));
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup_opts(&w, o, future - 1),
                                  (int)MOQ_ERR_INVAL);
            MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
            o->struct_size = (uint32_t)(v0 - 1);
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup_opts(&w, o, future),
                                  (int)MOQ_ERR_INVAL);
            o->struct_size = (uint32_t)v0;
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup_opts(&w, o, v0 - 1),
                                  (int)MOQ_ERR_INVAL);
            MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
            free(raw);
        }

        /* A failed decode clears the caller's record (stamped), never
         * leaving a partial result, and writes nothing past its size. */
        raw = malloc(v0 + 16);
        MOQ_TEST_CHECK(raw != NULL);
        if (raw) {
            memset(raw, 0xa5, v0 + 16);
            static const uint8_t dup[] = { 0x01, 0x01, 'a', 0x00, 0x01, 'b' };
            moq_d18_setup_opts_t *o = (moq_d18_setup_opts_t *)(void *)raw;
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_setup_opts(dup, sizeof(dup),
                o, v0), (int)MOQ_ERR_PROTO);
            MOQ_TEST_CHECK_EQ_U64(o->struct_size, v0);
            MOQ_TEST_CHECK(!o->has_path && !o->has_authority &&
                           o->auth_token_count == 0 && o->path.len == 0);
            bool canary = true;
            for (size_t i = v0; i < v0 + 16; i++) canary &= raw[i] == 0xa5;
            MOQ_TEST_CHECK(canary);
            free(raw);
        }

        /* init_sized clears and stamps only what it is given, inside a real
         * typed record between guards, inspected byte by byte: below the
         * size of struct_size nothing at all is written; from there exactly
         * `size` bytes are cleared and stamped. */
        static const size_t sizes[] = { 0, 1, 2, 3, 4, 8, 9,
                                        MOQ_D18_SETUP_OPTS_V0_SIZE,
                                        sizeof(moq_d18_setup_opts_t),
                                        sizeof(moq_d18_setup_opts_t) + 64 };
        for (size_t k = 0; k < sizeof(sizes) / sizeof(sizes[0]); k++) {
            struct init_guard {
                unsigned char        before[16];
                moq_d18_setup_opts_t rec;
                unsigned char        after[16];
            } g;
            memset(&g, 0xa5, sizeof(g));
            moq_d18_setup_opts_init_sized(&g.rec, sizes[k]);
            const unsigned char *bytes = (const unsigned char *)&g;
            size_t rec_at = offsetof(struct init_guard, rec);
            size_t written = sizes[k] < sizeof(g.rec.struct_size) ? 0
                           : sizes[k] < sizeof(g.rec) ? sizes[k] : sizeof(g.rec);
            bool exact = true;
            for (size_t i = 0; i < sizeof(g); i++) {
                bool inside = i >= rec_at && i < rec_at + written;
                bool in_stamp = inside && i < rec_at + sizeof(g.rec.struct_size);
                if (!inside) exact &= bytes[i] == 0xa5;
                else if (!in_stamp) exact &= bytes[i] == 0;
            }
            MOQ_TEST_CHECK(exact);
            if (written) {
                uint32_t stamped;
                memcpy(&stamped, bytes + rec_at, sizeof(stamped));
                MOQ_TEST_CHECK_EQ_U64(stamped, written);
            }
        }
        moq_d18_setup_opts_init_sized(NULL, sizeof(moq_d18_setup_opts_t));
        /* The plain form stamps the caller's own sizeof. */
        {
            moq_d18_setup_opts_t rec;
            memset(&rec, 0xa5, sizeof(rec));
            moq_d18_setup_opts_init(&rec);
            MOQ_TEST_CHECK_EQ_U64(rec.struct_size, sizeof(rec));
            MOQ_TEST_CHECK(!rec.has_path && rec.auth_token_count == 0);
        }

        /* No record at all is a SETUP with no options. */
        uint8_t buf[8];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup_opts(&w, NULL, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 4 && buf[0] == 0xaf &&
                       buf[1] == 0 && buf[2] == 0 && buf[3] == 0);
    }

    /* 1e. Duplicate known non-repeatable option closes. */
    {
        uint8_t p[] = { 0x04, 0x01, 0x00, 0x02 };   /* MAX=1, dup MAX=2 */
        moq_d18_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_setup_opts(p, sizeof(p), &o, sizeof(o)),
                              (int)MOQ_ERR_PROTO);
    }

    /* 1f. Over-cap odd value length (> 2^16-1) closes. */
    {
        uint8_t p[8];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0x01);                 /* PATH */
        put_vi64(&w, 0x10000);              /* length 65536: over cap */
        moq_d18_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_setup_opts(p, moq_buf_writer_offset(&w), &o, sizeof(o)),
            (int)MOQ_ERR_PROTO);
    }

    /* 1g. Malformed AUTHORIZATION_TOKEN structure -> KVP_FORMAT (0x6 path). */
    {
        uint8_t p[] = { 0x03, 0x01, 0x09 };   /* token option, len 1, alias_type 9 */
        moq_d18_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_setup_opts(p, sizeof(p), &o, sizeof(o)),
                              (int)MOQ_D18_ERR_KVP_FORMAT);
    }

    /* 1h. A REGISTER token option decodes its fields. */
    {
        uint8_t p[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0x03);
        put_vi64(&w, 1 + 1 + 1 + 5);        /* alias_type + alias + type + "alpha" */
        put_vi64(&w, MOQ_AUTH_TOKEN_REGISTER);
        put_vi64(&w, 1);                    /* alias */
        put_vi64(&w, 4);                    /* token type */
        moq_buf_write_raw(&w, (const uint8_t *)"alpha", 5);
        moq_d18_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_setup_opts(p, moq_buf_writer_offset(&w), &o, sizeof(o)),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(o.auth_token_count, 1);
        MOQ_TEST_CHECK_EQ_U64(o.auth_tokens[0].alias, 1);
        MOQ_TEST_CHECK_EQ_SIZE(o.auth_tokens[0].token_value.len, 5);
    }

    /* == 2. Session inbound SETUP ====================================== */

    /* 2a. SETUP with a REGISTER token + unknown option: establishes, surfaces
     * the token on SETUP_COMPLETE, and the alias is usable by a later request. */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_SERVER, 1024);
        MOQ_TEST_CHECK(s != NULL);
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        uint8_t p[48];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0x03);                              /* AUTH token option */
        put_vi64(&w, 1 + 1 + 1 + 5);
        put_vi64(&w, MOQ_AUTH_TOKEN_REGISTER);
        put_vi64(&w, 1); put_vi64(&w, 4);
        moq_buf_write_raw(&w, (const uint8_t *)"alpha", 5);
        put_vi64(&w, 0x04);                              /* delta -> 0x07 unknown odd */
        put_vi64(&w, 2);
        moq_buf_write_raw(&w, (const uint8_t *)"xy", 2);
        MOQ_TEST_CHECK_EQ_INT(
            (int)feed_setup(s, p, moq_buf_writer_offset(&w)), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        bool tok_ok = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE &&
                ev.u.setup_complete.token_count == 1 &&
                ev.u.setup_complete.tokens &&
                ev.u.setup_complete.tokens[0].token_value.len == 5 &&
                memcmp(ev.u.setup_complete.tokens[0].token_value.data,
                       "alpha", 5) == 0)
                tok_ok = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(tok_ok);
        /* The registered alias resolves in a later SUBSCRIBE (USE_ALIAS 1). */
        uint8_t sub[96];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, sub, sizeof(sub));
        put_vi64(&sw, 0x03);                /* SUBSCRIBE type */
        size_t len_off = moq_buf_writer_offset(&sw);
        moq_buf_write_uint16(&sw, 0);
        size_t body0 = moq_buf_writer_offset(&sw);
        put_vi64(&sw, 0);                   /* request id */
        put_ns1(&sw, "ns");
        put_span(&sw, "t");
        put_vi64(&sw, 1);                   /* one param */
        put_vi64(&sw, 0x03);                /* AUTH_TOKEN */
        put_vi64(&sw, 2);                   /* len: alias_type + alias */
        put_vi64(&sw, MOQ_AUTH_TOKEN_USE_ALIAS);
        put_vi64(&sw, 1);
        moq_buf_patch_uint16(&sw, len_off,
                             (uint16_t)(moq_buf_writer_offset(&sw) - body0));
        moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(0x51),
            sub, moq_buf_writer_offset(&sw), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        bool sub_tok = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST &&
                ev.u.subscribe_request.token_count == 1 &&
                ev.u.subscribe_request.tokens[0].token_value.len == 5 &&
                memcmp(ev.u.subscribe_request.tokens[0].token_value.data,
                       "alpha", 5) == 0)
                sub_tok = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(sub_tok);
        moq_session_destroy(s);
    }

    /* 2b. PATH direction rules: a client receiving PATH closes (INVALID_PATH);
     * a server receiving PATH records it and proceeds. */
    {
        uint8_t p[8];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0x01); put_vi64(&w, 1);
        moq_buf_write_raw(&w, (const uint8_t *)"/", 1);
        size_t plen = moq_buf_writer_offset(&w);

        moq_session_t *c = make_started(MOQ_PERSPECTIVE_CLIENT, 0);
        feed_setup(c, p, plen);
        MOQ_TEST_CHECK_EQ_INT((int)c->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(c);

        moq_session_t *sv = make_started(MOQ_PERSPECTIVE_SERVER, 0);
        feed_setup(sv, p, plen);
        MOQ_TEST_CHECK_EQ_INT((int)sv->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(sv->peer_setup.has_path);
        moq_session_destroy(sv);
    }

    /* 2c. DELETE token in SETUP closes; a malformed token closes. */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_SERVER, 1024);
        uint8_t p[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0x03); put_vi64(&w, 2);
        put_vi64(&w, MOQ_AUTH_TOKEN_DELETE); put_vi64(&w, 1);
        feed_setup(s, p, moq_buf_writer_offset(&w));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);

        moq_session_t *s2 = make_started(MOQ_PERSPECTIVE_SERVER, 1024);
        uint8_t bad[] = { 0x03, 0x01, 0x09 };
        feed_setup(s2, bad, sizeof(bad));
        MOQ_TEST_CHECK_EQ_INT((int)s2->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s2);
    }

    /* 2c2. A well-formed SETUP token with an empty value is opaque: the
     * session establishes and SETUP_COMPLETE surfaces the empty value
     * exactly; judging it is the application's authorization policy. */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_SERVER, 1024);
        uint8_t p[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0x03); put_vi64(&w, 2);
        put_vi64(&w, MOQ_AUTH_TOKEN_USE_VALUE);
        put_vi64(&w, 7);                       /* token type; empty value */
        feed_setup(s, p, moq_buf_writer_offset(&w));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        bool saw_close = false;
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_CLOSE_SESSION) saw_close = true;
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(!saw_close);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE &&
                ev.u.setup_complete.token_count == 1 &&
                ev.u.setup_complete.tokens[0].token_type == 7 &&
                ev.u.setup_complete.tokens[0].token_value.len == 0)
                got = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        moq_session_destroy(s);
    }

    /* 2e. Duplicate REGISTER alias in SETUP closes -- and the close STICKS
     * (regression: close_with_error inside token resolution returns MOQ_OK; the
     * handler must not fall through and establish a closed session). */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_SERVER, 1024);
        uint8_t p[48];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        for (int i = 0; i < 2; i++) {       /* two REGISTERs, same alias 1 */
            put_vi64(&w, i == 0 ? 0x03 : 0x00);   /* delta: 0x03, then dup type */
            put_vi64(&w, 1 + 1 + 1 + 1);
            put_vi64(&w, MOQ_AUTH_TOKEN_REGISTER);
            put_vi64(&w, 1); put_vi64(&w, 4);
            moq_buf_write_raw(&w, (const uint8_t *)"v", 1);
        }
        feed_setup(s, p, moq_buf_writer_offset(&w));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        bool complete = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) complete = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!complete);
        moq_session_destroy(s);
    }

    /* 2g. Sixteen SETUP tokens -- the most this side holds -- arrive whole; a
     * seventeenth is refused before anything is published or cached. The peer
     * broke no rule, so the close is the local-capacity INTERNAL_ERROR (0x1),
     * the same as draft-16. Both roles, USE_VALUE and REGISTER. */
    for (int server = 0; server < 2; server++) {
        for (int reg = 0; reg < 2; reg++) {
            for (size_t n = 16; n <= 17; n++) {
                moq_session_t *s = make_started(
                    server ? MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT, 4096);
                MOQ_TEST_CHECK(s != NULL);
                if (!s) continue;
                moq_action_t act;
                while (moq_session_poll_actions(s, &act, 1) > 0)
                    moq_action_cleanup(&act);
                uint8_t p[150];
                moq_buf_writer_t w;
                moq_buf_writer_init(&w, p, sizeof(p));
                for (size_t i = 0; i < n; i++) {
                    put_vi64(&w, i == 0 ? 0x03 : 0x00);
                    put_vi64(&w, reg ? 4 : 3);
                    put_vi64(&w, reg ? MOQ_AUTH_TOKEN_REGISTER
                                     : MOQ_AUTH_TOKEN_USE_VALUE);
                    if (reg) put_vi64(&w, i);           /* alias */
                    put_vi64(&w, 7);
                    uint8_t v = (uint8_t)('a' + i);
                    moq_buf_write_raw(&w, &v, 1);
                }
                feed_setup(s, p, moq_buf_writer_offset(&w));
                size_t seen = SIZE_MAX;
                moq_event_t ev;
                while (moq_session_poll_events(s, &ev, 1) > 0) {
                    if (ev.kind == MOQ_EVENT_SETUP_COMPLETE)
                        seen = ev.u.setup_complete.token_count;
                    moq_event_cleanup(&ev);
                }
                uint64_t code = UINT64_MAX;
                while (moq_session_poll_actions(s, &act, 1) > 0) {
                    if (act.kind == MOQ_ACTION_CLOSE_SESSION)
                        code = act.u.close_session.code;
                    moq_action_cleanup(&act);
                }
                if (n == 16) {
                    MOQ_TEST_CHECK_EQ_SIZE(seen, 16);
                    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
                } else {
                    MOQ_TEST_CHECK_EQ_SIZE(seen, SIZE_MAX);
                    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
                    MOQ_TEST_CHECK_EQ_U64(code, 0x1);
                    MOQ_TEST_CHECK_EQ_SIZE(s->peer_token_cache.used_bytes, 0);
                }
                moq_session_destroy(s);
            }
        }
    }

    /* 2f. §10.3.1.4: a SETUP REGISTER exceeding the advertised cache size
     * (here the default 0) MUST NOT fail the session -- it is treated as
     * USE_VALUE: established, token surfaced, alias NOT registered. */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_SERVER, 0);
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        uint8_t p[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0x03);
        put_vi64(&w, 1 + 1 + 1 + 5);
        put_vi64(&w, MOQ_AUTH_TOKEN_REGISTER);
        put_vi64(&w, 1); put_vi64(&w, 4);
        moq_buf_write_raw(&w, (const uint8_t *)"alpha", 5);
        MOQ_TEST_CHECK_EQ_INT(
            (int)feed_setup(s, p, moq_buf_writer_offset(&w)), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        bool tok = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE &&
                ev.u.setup_complete.token_count == 1 &&
                ev.u.setup_complete.tokens[0].token_value.len == 5)
                tok = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(tok);
        /* The alias was NOT registered: a later USE_ALIAS is rejected (the
         * request fails; the session stays alive). */
        uint8_t sub[96];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, sub, sizeof(sub));
        put_vi64(&sw, 0x03);
        size_t len_off = moq_buf_writer_offset(&sw);
        moq_buf_write_uint16(&sw, 0);
        size_t body0 = moq_buf_writer_offset(&sw);
        put_vi64(&sw, 0);
        put_ns1(&sw, "ns"); put_span(&sw, "t");
        put_vi64(&sw, 1);
        put_vi64(&sw, 0x03);
        put_vi64(&sw, 2);
        put_vi64(&sw, MOQ_AUTH_TOKEN_USE_ALIAS);
        put_vi64(&sw, 1);
        moq_buf_patch_uint16(&sw, len_off,
                             (uint16_t)(moq_buf_writer_offset(&sw) - body0));
        moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(0x52),
            sub, moq_buf_writer_offset(&sw), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        bool surfaced = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) surfaced = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!surfaced);   /* unknown alias: request auto-rejected */
        moq_session_destroy(s);
    }

    /* 2d. The peer's advertised cache size is surfaced. */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_SERVER, 0);
        uint8_t p[] = { 0x04, 0x64 };
        feed_setup(s, p, sizeof(p));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_U64(moq_session_peer_auth_token_cache_size(s), 100);
        moq_session_destroy(s);
    }

    /* == 3. Outbound SETUP option emission ============================= */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_CLIENT, 100);
        bool found = false;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_OPEN_UNI_CONTROL) {
                const uint8_t *d = a.u.open_uni_control.data;
                size_t n = a.u.open_uni_control.len;
                /* SETUP payload follows the vi64 type + 16-bit length; assert
                 * the vi64 single-byte value form at the boundary (100). */
                MOQ_TEST_CHECK(n >= 4);
                MOQ_TEST_CHECK_EQ_HEX(d[n - 2], 0x04);
                MOQ_TEST_CHECK_EQ_HEX(d[n - 1], 0x64);
                moq_d18_setup_opts_t o;
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_setup_opts(d + (n - 2), 2, &o, sizeof(o)),
                    (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(o.max_auth_token_cache_size, 100);
                found = true;
            }
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK(found);
        moq_session_destroy(s);

        /* Without the cfg, the SETUP payload stays empty. */
        moq_session_t *s2 = make_started(MOQ_PERSPECTIVE_CLIENT, 0);
        bool empty_ok = false;
        while (moq_session_poll_actions(s2, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_OPEN_UNI_CONTROL) {
                size_t n = a.u.open_uni_control.len;
                const uint8_t *d = a.u.open_uni_control.data;
                /* last two bytes are the 16-bit zero Length */
                empty_ok = (d[n - 1] == 0x00 && d[n - 2] == 0x00);
            }
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK(empty_ok);
        moq_session_destroy(s2);
    }

    /* == 4. Defined-but-unmodeled message parameters =================== */

    /* 4a. SUBSCRIBE with RENDEZVOUS_TIMEOUT (0x04) + NEW_GROUP_REQUEST (0x32):
     * accepted (decoded + dropped). */
    {
        uint8_t p[96];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0);                    /* request id */
        put_ns1(&w, "ns"); put_span(&w, "t");
        put_vi64(&w, 2);
        put_vi64(&w, 0x04); put_vi64(&w, 1000);          /* RENDEZVOUS */
        put_vi64(&w, 0x32 - 0x04); put_vi64(&w, 7);      /* NGR */
        moq_bytes_t parts[4];
        moq_d18_subscribe_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe(p, moq_buf_writer_offset(&w),
                                          parts, 4, &out), (int)MOQ_OK);
    }

    /* 4b. FILL_TIMEOUT in SUBSCRIBE is out of scope -> PROTO. */
    {
        uint8_t p[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0);
        put_ns1(&w, "ns"); put_span(&w, "t");
        put_vi64(&w, 1);
        put_vi64(&w, 0x0A); put_vi64(&w, 5);
        moq_bytes_t parts[4];
        moq_d18_subscribe_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe(p, moq_buf_writer_offset(&w),
                                          parts, 4, &out), (int)MOQ_ERR_PROTO);
    }

    /* 4c. FETCH with FILL_TIMEOUT (0x0A): accepted. */
    {
        uint8_t p[96];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 2);                    /* request id */
        put_vi64(&w, 1);                    /* standalone */
        put_ns1(&w, "ns"); put_span(&w, "t");
        put_vi64(&w, 0); put_vi64(&w, 0);   /* start */
        put_vi64(&w, 1); put_vi64(&w, 0);   /* end */
        put_vi64(&w, 1);
        put_vi64(&w, 0x0A); put_vi64(&w, 5);
        moq_bytes_t parts[4];
        moq_d18_fetch_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_fetch(p, moq_buf_writer_offset(&w),
                                      parts, 4, &out), (int)MOQ_OK);
    }

    /* 4d. REQUEST_UPDATE with NEW_GROUP_REQUEST + TRACK_NAMESPACE_PREFIX:
     * accepted. A zero-field (root) prefix is legal (§10.2.14: 0..32 fields);
     * more than 32 fields rejects. */
    {
        uint8_t p[160];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0);                    /* request id */
        put_vi64(&w, 2);
        put_vi64(&w, 0x32); put_vi64(&w, 3);             /* NGR */
        put_vi64(&w, 0x34 - 0x32);                       /* TNP */
        put_vi64(&w, 1);                                 /* 1 part */
        put_vi64(&w, 2);
        moq_buf_write_raw(&w, (const uint8_t *)"ab", 2);
        moq_d18_request_update_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_request_update(p, moq_buf_writer_offset(&w),
                                               &out), (int)MOQ_OK);

        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0);
        put_vi64(&w, 1);
        put_vi64(&w, 0x34);
        put_vi64(&w, 0);                                 /* 0 parts: root prefix */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_request_update(p, moq_buf_writer_offset(&w),
                                               &out), (int)MOQ_OK);

        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0);
        put_vi64(&w, 1);
        put_vi64(&w, 0x34);
        put_vi64(&w, 33);                                /* over the 32 cap */
        for (int i = 0; i < 33; i++) {
            put_vi64(&w, 1);
            moq_buf_write_raw(&w, (const uint8_t *)"a", 1);
        }
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_request_update(p, moq_buf_writer_offset(&w),
                                               &out), (int)MOQ_ERR_PROTO);
    }

    /* 4e. PUBLISH_OK with NEW_GROUP_REQUEST: accepted. */
    {
        uint8_t p[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 1);
        put_vi64(&w, 0x32); put_vi64(&w, 9);
        moq_d18_publish_ok_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_publish_ok(p, moq_buf_writer_offset(&w), &out),
            (int)MOQ_OK);
    }

    /* 4f. A truly unknown parameter type still closes. */
    {
        uint8_t p[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0);
        put_ns1(&w, "ns"); put_span(&w, "t");
        put_vi64(&w, 1);
        put_vi64(&w, 0x66); put_vi64(&w, 1);
        moq_bytes_t parts[4];
        moq_d18_subscribe_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe(p, moq_buf_writer_offset(&w),
                                          parts, 4, &out), (int)MOQ_ERR_PROTO);
    }

    /* 4g. End-to-end: a server receiving a SUBSCRIBE that carries the accepted
     * params surfaces SUBSCRIBE_REQUEST and stays alive. */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_SERVER, 0);
        feed_setup(s, NULL, 0);
        { moq_event_t e; while (moq_session_poll_events(s,&e,1)>0) moq_event_cleanup(&e); }
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        uint8_t m[96];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, m, sizeof(m));
        put_vi64(&w, 0x03);                 /* SUBSCRIBE */
        size_t len_off = moq_buf_writer_offset(&w);
        moq_buf_write_uint16(&w, 0);
        size_t body0 = moq_buf_writer_offset(&w);
        put_vi64(&w, 0);
        put_ns1(&w, "ns"); put_span(&w, "t");
        put_vi64(&w, 2);
        put_vi64(&w, 0x04); put_vi64(&w, 1000);
        put_vi64(&w, 0x32 - 0x04); put_vi64(&w, 7);
        moq_buf_patch_uint16(&w, len_off,
                             (uint16_t)(moq_buf_writer_offset(&w) - body0));
        moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(0x61),
            m, moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) got = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        moq_session_destroy(s);
    }

    /* == 5. Descending FETCH guard ===================================== */

    /* 5a. The ascending control: the same request with GROUP_ORDER 1. */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_CLIENT, 0);
        feed_setup(s, NULL, 0);
        { moq_event_t e; while (moq_session_poll_events(s,&e,1)>0) moq_event_cleanup(&e); }
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        moq_fetch_cfg_t fc;
        moq_fetch_cfg_init(&fc);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("ns") };
        fc.track_namespace = (moq_namespace_t){ parts, 1 };
        fc.track_name = MOQ_BYTES_LITERAL("t");
        fc.end_group = 1;
        fc.group_order = MOQ_GROUP_ORDER_ASCENDING;
        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(s, &fc, 1, &h), (int)MOQ_OK);
        static const uint8_t want_fetch[] = {
            0x16, 0x00, 0x0f, 0x00, 0x01, 0x01, 0x02, 'n', 's', 0x01, 't',
            0x00, 0x00, 0x01, 0x00, 0x01, 0x22, 0x01
        };
        bool sent = false;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
                sent = a.u.open_bidi_stream.len == sizeof(want_fetch) &&
                       memcmp(a.u.open_bidi_stream.data, want_fetch, sizeof(want_fetch)) == 0;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK(sent);
        moq_session_destroy(s);
    }

    /* 5b. Draft-18 originates a descending FETCH and reads its response
     * (10.12, 10.13, 11.4.4): the request is exactly the bytes below, and a
     * hand-written response of groups 1 then 0 -- Group ID Delta read as
     * prior - (delta + 1) when descending (11.4.4.1) -- surfaces as those
     * Objects, in that order, then the completion. Every value fits a
     * one-byte vi64 (1.4.1). */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_CLIENT, 0);
        feed_setup(s, NULL, 0);
        { moq_event_t e; while (moq_session_poll_events(s,&e,1)>0) moq_event_cleanup(&e); }
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        moq_fetch_cfg_t fc;
        moq_fetch_cfg_init(&fc);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("ns") };
        fc.track_namespace = (moq_namespace_t){ parts, 1 };
        fc.track_name = MOQ_BYTES_LITERAL("t");
        fc.end_group = 1;
        fc.group_order = MOQ_GROUP_ORDER_DESCENDING;
        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(s, &fc, 1, &h), (int)MOQ_OK);
        /* FETCH: Type 0x16, Length 15, Request ID 0, Standalone, Namespace
         * {ns}, Name t, Start 0:0, End 1:0 (all of group 1), one Parameter:
         * GROUP_ORDER (0x22) = 2. */
        static const uint8_t want_fetch[] = {
            0x16, 0x00, 0x0f, 0x00, 0x01, 0x01, 0x02, 'n', 's', 0x01, 't',
            0x00, 0x00, 0x01, 0x00, 0x01, 0x22, 0x02
        };
        moq_stream_ref_t req = { 0 };
        bool sent = false;
        {
            moq_action_t a;
            while (moq_session_poll_actions(s, &a, 1) > 0) {
                if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                    req = a.u.open_bidi_stream.stream_ref;
                    sent = a.u.open_bidi_stream.len == sizeof(want_fetch) &&
                           memcmp(a.u.open_bidi_stream.data, want_fetch,
                                  sizeof(want_fetch)) == 0;
                }
                moq_action_cleanup(&a);
            }
        }
        MOQ_TEST_CHECK(sent);
        /* FETCH_OK: Type 0x18, Length 4, End Of Track 0, End 1:0, no
         * Parameters, no Track Properties. */
        static const uint8_t ok[] = { 0x18, 0x00, 0x04, 0x00, 0x01, 0x00, 0x00 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(s, req, ok, sizeof(ok),
                                                                    false, 2), (int)MOQ_OK);
        /* FETCH_HEADER (0x05, Request ID 0), then
         *   1:0  flags 0x1C (Group ID Delta, Object ID Delta, Priority):
         *        Group 1, Object 0, priority 0x80, payload "a"
         *   1:1  flags 0x00: same group, Object prior + 1, payload "b"
         *   0:0  flags 0x0C: Group 1 - (0 + 1) = 0, Object 0, payload "c"
         *   0:1  flags 0x00: payload "d" */
        static const uint8_t data[] = {
            0x05, 0x00,
            0x1C, 0x01, 0x00, 0x80, 0x01, 'a',
            0x00, 0x01, 'b',
            0x0C, 0x00, 0x00, 0x01, 'c',
            0x00, 0x01, 'd'
        };
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_bytes(s, moq_stream_ref_from_u64(0x51),
                                                             data, sizeof(data), true, 3),
                              (int)MOQ_OK);
        static const uint64_t wg[] = { 1, 1, 0, 0 }, wo[] = { 0, 1, 0, 1 };
        static const char wp[] = { 'a', 'b', 'c', 'd' };
        int n_ok = 0, n_obj = 0, n_done = 0, bad = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_OK) {
                n_ok++;
                bad += ev.u.fetch_ok.end_group != 1 || ev.u.fetch_ok.end_object != 0;
            } else if (ev.kind == MOQ_EVENT_FETCH_OBJECT) {
                const moq_fetch_object_event_t *fo = &ev.u.fetch_object;
                bad += n_obj >= 4 || fo->group_id != wg[n_obj] || fo->object_id != wo[n_obj] ||
                       fo->payload == NULL || moq_rcbuf_len(fo->payload) != 1 ||
                       moq_rcbuf_data(fo->payload)[0] != (uint8_t)wp[n_obj];
                n_obj++;
            } else if (ev.kind == MOQ_EVENT_FETCH_COMPLETE) {
                n_done++;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(n_ok == 1 && n_obj == 4 && n_done == 1 && bad == 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* 5c. Draft-16 still accepts DESCENDING (absolute group ids on the wire). */
    {
        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = moq_alloc_default();
        cfg.version = MOQ_VERSION_DRAFT_16;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 10;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 10;
        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_create(&cfg, &sp), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        moq_fetch_cfg_t fc;
        moq_fetch_cfg_init(&fc);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("ns") };
        fc.track_namespace = (moq_namespace_t){ parts, 1 };
        fc.track_name = MOQ_BYTES_LITERAL("t");
        fc.end_group = 1;
        fc.group_order = MOQ_GROUP_ORDER_DESCENDING;
        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_fetch(moq_simpair_client(sp), &fc,
                                   moq_simpair_now_us(sp), &h), (int)MOQ_OK);
        moq_simpair_destroy(sp);
    }

    /* == The SETUP handler always sees an empty scratch arena ==========
     * An early request bidi may be torn down before establishment, queuing an
     * event ahead of SETUP -- so the event queue need not be empty when SETUP
     * arrives. That event carries no borrowed bytes, and every other
     * event-scratch producer needs an active session, so the arena cursor is
     * still zero when the handler weighs the completion event's tokens. */
    {
        moq_session_t *sv = make_started(MOQ_PERSPECTIVE_SERVER, 0);
        MOQ_TEST_CHECK(sv != NULL);
        if (sv) {
            moq_stream_ref_t ref = { 0x40 };
            uint8_t sub[64];
            moq_buf_writer_t sw;
            moq_buf_writer_init(&sw, sub, sizeof(sub));
            moq_buf_write_vi64(&sw, 0x20);            /* SUBSCRIBE */
            moq_buf_write_uint16(&sw, 0);             /* empty payload */
            MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, ref, sub,
                               moq_buf_writer_offset(&sw), false, 0) == MOQ_OK);
            MOQ_TEST_CHECK(moq_session_on_bidi_stream_reset(sv, ref, 0x1, 0)
                           == MOQ_OK);

            /* The teardown really did surface an event, and it borrowed no
             * scratch. */
            MOQ_TEST_CHECK(sv->event_head != sv->event_tail);
            MOQ_TEST_CHECK(sv->event_scratch_len == 0);

            /* SETUP is processed with the arena still empty. */
            uint8_t opts[8];
            moq_buf_writer_t ow;
            moq_buf_writer_init(&ow, opts, sizeof(opts));
            MOQ_TEST_CHECK(feed_setup(sv, opts,
                                      moq_buf_writer_offset(&ow)) == MOQ_OK);
            MOQ_TEST_CHECK(sv->event_scratch_len == 0);
            MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

            /* The teardown event precedes the completion, and nothing else
             * follows. */
            moq_event_t e;
            memset(&e, 0, sizeof(e));
            MOQ_TEST_CHECK(moq_session_poll_events(sv, &e, 1) == 1);
            MOQ_TEST_CHECK(e.kind == MOQ_EVENT_UNSUBSCRIBED);
            moq_event_cleanup(&e);
            memset(&e, 0, sizeof(e));
            MOQ_TEST_CHECK(moq_session_poll_events(sv, &e, 1) == 1);
            MOQ_TEST_CHECK(e.kind == MOQ_EVENT_SETUP_COMPLETE);
            moq_event_cleanup(&e);
            memset(&e, 0, sizeof(e));
            MOQ_TEST_CHECK(moq_session_poll_events(sv, &e, 1) == 0);

            moq_session_destroy(sv);
        }
    }

    /* == A SETUP token requirement beyond the arena closes normally ====
     * Regression for the shared classification: the shortfall must end the
     * session with the internal-limit close, never a buffer error. */
    {
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_SERVER);
        cfg.version = MOQ_VERSION_DRAFT_18;
        cfg.output_scratch_size = 32;
        moq_session_t *sv = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &sv) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_start(sv, 0) == MOQ_OK);
        if (sv) {
            uint8_t val[40];
            for (size_t i = 0; i < sizeof(val); i++)
                val[i] = (uint8_t)(0x41 + (i % 26));

            /* One USE_VALUE token option whose literal dwarfs the arena. */
            uint8_t payload[160];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, payload, sizeof(payload));
            put_vi64(&w, 0x03);                       /* token option        */
            put_vi64(&w, 1 + 1 + sizeof(val));        /* alias_type+type+val */
            put_vi64(&w, MOQ_AUTH_TOKEN_USE_VALUE);
            put_vi64(&w, 0);                          /* token type          */
            moq_buf_write_raw(&w, val, sizeof(val));

            moq_result_t rc = feed_setup(sv, payload,
                                         moq_buf_writer_offset(&w));
            MOQ_TEST_CHECK(rc == MOQ_OK);
            MOQ_TEST_CHECK(rc != MOQ_ERR_BUFFER);
            MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

            moq_action_t a;
            memset(&a, 0, sizeof(a));
            MOQ_TEST_CHECK(moq_session_poll_actions(sv, &a, 1) == 1);
            MOQ_TEST_CHECK(a.kind == MOQ_ACTION_CLOSE_SESSION);
            MOQ_TEST_CHECK(a.u.close_session.code == 0x1);
            MOQ_TEST_CHECK(a.u.close_session.reason.len ==
                           strlen("event scratch permanently too small"));
            MOQ_TEST_CHECK(memcmp(a.u.close_session.reason.data,
                                  "event scratch permanently too small",
                                  a.u.close_session.reason.len) == 0);
            moq_action_cleanup(&a);
            moq_session_destroy(sv);
        }
    }

    MOQ_TEST_PASS("d18_setup_options");
    return failures != 0;
}
