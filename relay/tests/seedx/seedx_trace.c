#include <stdio.h>
#include <string.h>

#include "seedx_trace.h"

const char *
seedx_trace_err_name(seedx_trace_err_t e)
{
    switch (e) {
    case SEEDX_TRACE_OK:          return "ok";
    case SEEDX_TRACE_E_MAGIC:     return "bad-magic";
    case SEEDX_TRACE_E_VERSION:   return "bad-version";
    case SEEDX_TRACE_E_PRNG:      return "bad-prng-id";
    case SEEDX_TRACE_E_TRUNCATED: return "truncated";
    case SEEDX_TRACE_E_TRAILING:  return "trailing-bytes";
    case SEEDX_TRACE_E_COUNT:     return "bad-op-count";
    case SEEDX_TRACE_E_CRC:       return "bad-crc";
    case SEEDX_TRACE_E_CONFIG:    return "config-hash-mismatch";
    case SEEDX_TRACE_E_OP:        return "unknown-op";
    case SEEDX_TRACE_E_FIELD:     return "field-out-of-range";
    }
    return "unknown-error";
}

/* CRC-32/ISO-HDLC: reflected poly 0xEDB88320, init 0xFFFFFFFF (passed in as
 * `running` pre-inverted by the ~0u seed below), reflected in/out, final XOR
 * applied by the caller-facing wrappers here. */
uint32_t
seedx_crc32(const uint8_t *data, size_t len, uint32_t running)
{
    uint32_t c = running;

    for (size_t i = 0; i < len; i++) {
        c ^= data[i];
        for (int k = 0; k < 8; k++) {
            c = (c >> 1) ^ (0xEDB88320u & (0u - (c & 1u)));
        }
    }
    return c;
}

static void
put16(uint8_t *p, uint16_t v) { p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8); }
static void
put32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16); p[3] = (uint8_t)(v >> 24);
}
static void
put64(uint8_t *p, uint64_t v) { put32(p, (uint32_t)v); put32(p + 4, (uint32_t)(v >> 32)); }
static uint16_t
get16(const uint8_t *p) { return (uint16_t)(p[0] | ((uint16_t)p[1] << 8)); }
static uint32_t
get32(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) |
           ((uint32_t)p[3] << 24);
}
static uint64_t
get64(const uint8_t *p) { return (uint64_t)get32(p) | ((uint64_t)get32(p + 4) << 32); }

static void
rec_write(uint8_t *p, const seedx_rec_t *r)
{
    p[0] = r->op;
    p[1] = r->lane;
    put16(p + 2, r->a);
    put16(p + 4, r->b);
    put16(p + 6, r->c);
}

/* crc over the header with its crc field zeroed, then every record byte */
static uint32_t
trace_crc(const uint8_t *buf, size_t len)
{
    uint8_t hdr[SEEDX_TRACE_HDR];

    memcpy(hdr, buf, SEEDX_TRACE_HDR);
    memset(hdr + 28, 0, 4);
    uint32_t c = seedx_crc32(hdr, SEEDX_TRACE_HDR, 0xFFFFFFFFu);
    c = seedx_crc32(buf + SEEDX_TRACE_HDR, len - SEEDX_TRACE_HDR, c);
    return c ^ 0xFFFFFFFFu;
}

size_t
seedx_trace_encode(uint64_t seed, uint64_t config_hash,
                   const seedx_rec_t *recs, uint32_t count, uint8_t *out,
                   size_t cap)
{
    size_t need = SEEDX_TRACE_HDR + (size_t)count * SEEDX_TRACE_REC;

    if (cap < need || count > SEEDX_TRACE_MAX_OPS) {
        return 0;
    }
    put32(out, SEEDX_TRACE_MAGIC);
    put16(out + 4, SEEDX_TRACE_VERSION);
    put16(out + 6, SEEDX_TRACE_PRNG_ID);
    put64(out + 8, seed);
    put32(out + 16, count);
    put64(out + 20, config_hash);
    memset(out + 28, 0, 4);
    for (uint32_t i = 0; i < count; i++) {
        rec_write(out + SEEDX_TRACE_HDR + (size_t)i * SEEDX_TRACE_REC,
                  &recs[i]);
    }
    put32(out + 28, trace_crc(out, need));
    return need;
}

seedx_trace_err_t
seedx_trace_decode(const uint8_t *buf, size_t len,
                   uint64_t expect_config_hash, uint8_t max_op,
                   seedx_field_ok_fn ok, seedx_rec_t *arena,
                   uint32_t arena_cap, seedx_trace_t *out)
{
    if (len < SEEDX_TRACE_HDR) {
        return SEEDX_TRACE_E_TRUNCATED;
    }
    if (get32(buf) != SEEDX_TRACE_MAGIC) {
        return SEEDX_TRACE_E_MAGIC;
    }
    if (get16(buf + 4) != SEEDX_TRACE_VERSION) {
        return SEEDX_TRACE_E_VERSION;
    }
    if (get16(buf + 6) != SEEDX_TRACE_PRNG_ID) {
        return SEEDX_TRACE_E_PRNG;
    }
    uint32_t count = get32(buf + 16);

    if (count > SEEDX_TRACE_MAX_OPS || count > arena_cap) {
        return SEEDX_TRACE_E_COUNT;
    }
    size_t need = SEEDX_TRACE_HDR + (size_t)count * SEEDX_TRACE_REC;

    if (len < need) {
        return SEEDX_TRACE_E_TRUNCATED;
    }
    if (len > need) {
        return SEEDX_TRACE_E_TRAILING;
    }
    if (get32(buf + 28) != trace_crc(buf, need)) {
        return SEEDX_TRACE_E_CRC;
    }
    if (get64(buf + 20) != expect_config_hash) {
        return SEEDX_TRACE_E_CONFIG;
    }
    for (uint32_t i = 0; i < count; i++) {
        const uint8_t *p = buf + SEEDX_TRACE_HDR + (size_t)i * SEEDX_TRACE_REC;
        seedx_rec_t *r = &arena[i];

        r->op = p[0];
        r->lane = p[1];
        r->a = get16(p + 2);
        r->b = get16(p + 4);
        r->c = get16(p + 6);
        if (r->op > max_op) {
            return SEEDX_TRACE_E_OP;
        }
        if (ok != NULL && !ok(r)) {
            return SEEDX_TRACE_E_FIELD;
        }
    }
    out->seed = get64(buf + 8);
    out->config_hash = get64(buf + 20);
    out->op_count = count;
    out->recs = arena;
    return SEEDX_TRACE_OK;
}

size_t
seedx_trace_render(const seedx_trace_t *t, char *out, size_t cap)
{
    size_t w = 0;
    int n = snprintf(out, cap,
                     "MQRT v%u prng=%u seed=0x%016llx ops=%u cfg=0x%016llx\n",
                     SEEDX_TRACE_VERSION, SEEDX_TRACE_PRNG_ID,
                     (unsigned long long)t->seed, t->op_count,
                     (unsigned long long)t->config_hash);

    if (n < 0) {
        return 0;
    }
    w = (size_t)n;
    for (uint32_t i = 0; i < t->op_count && w < cap; i++) {
        const seedx_rec_t *r = &t->recs[i];

        n = snprintf(out + w, cap - w, "%04u op=%02x lane=%u a=%u b=%u c=%u\n",
                     i, r->op, r->lane, r->a, r->b, r->c);
        if (n < 0) {
            return w;
        }
        w += (size_t)n;
    }
    return w;
}
