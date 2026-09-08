#ifndef MOQR_SEEDX_TRACE_H
#define MOQR_SEEDX_TRACE_H

/*
 * The one canonical trace encoding — the ONLY replayable form. Little-endian
 * throughout. A trace file is:
 *
 *   header (32 bytes):
 *     0  u32  magic   "MQRT" (0x4D 0x51 0x52 0x54 as bytes)
 *     4  u16  version (1)
 *     6  u16  prng_id (1 = splitmix64-v1)
 *     8  u64  seed
 *    16  u32  op_count
 *    20  u64  config_hash   (FNV-1a/64 over the rig constants)
 *    28  u32  crc32         (see below)
 *   records (8 bytes each, op_count of them):
 *     0  u8   op
 *     1  u8   lane
 *     2  u16  a
 *     4  u16  b
 *     6  u16  c
 *
 * crc32 is CRC-32/ISO-HDLC exactly: reflected polynomial 0xEDB88320, initial
 * value 0xFFFFFFFF, reflected input and output, final XOR 0xFFFFFFFF —
 * computed over the HEADER WITH ITS CRC FIELD AS FOUR ZERO BYTES followed by
 * every record byte. A checksum that covered records alone would let a
 * corrupted header replay silently.
 *
 * Decoding is strict and fail-closed: wrong magic/version/prng, truncation,
 * trailing bytes, op_count overflowing the actual byte length, an unknown op,
 * or any 16-bit field outside its op's declared range is a named error.
 * Nothing malformed is ever demoted to a skip. The human renderer is one
 * deterministic line per record and is never parsed back.
 */

#include <stddef.h>
#include <stdint.h>

#define SEEDX_TRACE_MAGIC   0x5452514Du /* "MQRT" little-endian */
#define SEEDX_TRACE_VERSION 1u
#define SEEDX_TRACE_PRNG_ID 1u
#define SEEDX_TRACE_HDR     32u
#define SEEDX_TRACE_REC     8u
#define SEEDX_TRACE_MAX_OPS 65536u

typedef struct seedx_rec {
    uint8_t  op;
    uint8_t  lane;
    uint16_t a, b, c;
} seedx_rec_t;

typedef struct seedx_trace {
    uint64_t     seed;
    uint64_t     config_hash;
    uint32_t     op_count;
    seedx_rec_t *recs; /* borrowed from the caller-provided arena */
} seedx_trace_t;

typedef enum {
    SEEDX_TRACE_OK = 0,
    SEEDX_TRACE_E_MAGIC,
    SEEDX_TRACE_E_VERSION,
    SEEDX_TRACE_E_PRNG,
    SEEDX_TRACE_E_TRUNCATED,
    SEEDX_TRACE_E_TRAILING,
    SEEDX_TRACE_E_COUNT,     /* op_count vs byte length, or > MAX_OPS   */
    SEEDX_TRACE_E_CRC,
    SEEDX_TRACE_E_CONFIG,    /* config_hash mismatch with this rig      */
    SEEDX_TRACE_E_OP,        /* unknown op code                         */
    SEEDX_TRACE_E_FIELD,     /* a 16-bit field outside its op's range   */
} seedx_trace_err_t;

const char *seedx_trace_err_name(seedx_trace_err_t e);

uint32_t seedx_crc32(const uint8_t *data, size_t len, uint32_t running);

/* Encode into caller buffer (cap >= HDR + count*REC). Returns bytes written. */
size_t seedx_trace_encode(uint64_t seed, uint64_t config_hash,
                          const seedx_rec_t *recs, uint32_t count,
                          uint8_t *out, size_t cap);

/* Strict decode. `max_op` and `field_ok` let the caller supply the op
 * vocabulary so the codec never drifts from the executor's table. */
typedef int (*seedx_field_ok_fn)(const seedx_rec_t *r);
seedx_trace_err_t seedx_trace_decode(const uint8_t *buf, size_t len,
                                     uint64_t expect_config_hash,
                                     uint8_t max_op, seedx_field_ok_fn ok,
                                     seedx_rec_t *arena, uint32_t arena_cap,
                                     seedx_trace_t *out);

/* Deterministic human renderer: one line per record into `out`. */
size_t seedx_trace_render(const seedx_trace_t *t, char *out, size_t cap);

#endif /* MOQR_SEEDX_TRACE_H */
