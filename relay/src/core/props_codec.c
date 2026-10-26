#include "props_codec_internal.h"

#include <moq/vi64.h>
#include <moq/wire.h>

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#define PROPS_IMMUTABLE_TYPE 0xBu
#define PROPS_MAX_LENGTH     0xFFFFu

static bool codec_valid(uint8_t codec)
{
    return codec == MOQR_PROPS_CODEC_QUIC_VARINT || codec == MOQR_PROPS_CODEC_VI64;
}

/* One integer: the bytes consumed (0 on truncation). */
static size_t int_read(uint8_t codec, const uint8_t *p, size_t n, uint64_t *v)
{
    if (n == 0) return 0;
    return codec == MOQR_PROPS_CODEC_VI64 ? moq_vi64_decode(p, n, v)
                                          : moq_quic_varint_decode(p, n, v);
}

/* Its minimal encoding's size in `codec`, 0 when it cannot be represented. */
static size_t int_len(uint8_t codec, uint64_t v)
{
    if (codec == MOQR_PROPS_CODEC_QUIC_VARINT)
        return v > MOQ_QUIC_VARINT_MAX ? 0 : moq_quic_varint_len(v);
    return moq_vi64_len(v);
}

static size_t int_write(uint8_t codec, uint64_t v, uint8_t *out, size_t cap)
{
    return codec == MOQR_PROPS_CODEC_VI64 ? moq_vi64_encode(v, out, cap)
                                          : moq_quic_varint_encode(v, out, cap);
}

/* One Key-Value-Pair as read. */
typedef struct props_kvp {
    uint64_t       type;      /* absolute                                    */
    uint64_t       delta;
    uint64_t       value;     /* even Type                                   */
    const uint8_t *bytes;     /* odd Type: Length bytes                       */
    uint64_t       length;
    size_t         header;    /* bytes of Delta Type (+ Length)              */
    size_t         size;      /* the whole entry                             */
} props_kvp_t;

/* Read the entry at p[0..n): 1 read, 0 at the end, -1 malformed. */
static int kvp_read(uint8_t codec, const uint8_t *p, size_t n, uint64_t prev,
                    props_kvp_t *k)
{
    if (n == 0) return 0;
    size_t used = int_read(codec, p, n, &k->delta);
    if (used == 0) return -1;
    if (k->delta > UINT64_MAX - prev) return -1;   /* Type past 2^64-1 */
    k->type = prev + k->delta;
    if ((k->type & 1u) == 0) {
        size_t vl = int_read(codec, p + used, n - used, &k->value);
        if (vl == 0) return -1;
        k->bytes = NULL;
        k->length = 0;
        k->header = used;
        k->size = used + vl;
        return 1;
    }
    size_t ll = int_read(codec, p + used, n - used, &k->length);
    if (ll == 0 || k->length > PROPS_MAX_LENGTH) return -1;
    used += ll;
    if (k->length > n - used) return -1;
    k->bytes = p + used;
    k->header = used;
    k->size = used + (size_t)k->length;
    return 1;
}

/* Whether the Key-Value-Pairs Immutable Properties protect are valid in
 * their own codec: every pair parses, the bytes are consumed exactly, and
 * none is itself Immutable Properties (draft-16 11.2, draft-18 12.7 make
 * both malformed). */
static bool block_valid(uint8_t codec, const uint8_t *p, size_t len)
{
    uint64_t prev = 0;
    size_t at = 0;
    for (;;) {
        props_kvp_t k;
        int r = kvp_read(codec, p + at, len - at, prev, &k);
        if (r < 0) return false;
        if (r == 0) return true;
        if (k.type == PROPS_IMMUTABLE_TYPE) return false;
        prev = k.type;
        at += k.size;
    }
}

/* Whether `len` bytes, valid in one codec, read as the same Key-Value-Pairs
 * in both. */
static bool block_reads_alike(const uint8_t *p, size_t len)
{
    uint64_t pa = 0, pb = 0;
    size_t oa = 0, ob = 0;
    for (;;) {
        props_kvp_t a, b;
        int ra = kvp_read(MOQR_PROPS_CODEC_QUIC_VARINT, p + oa, len - oa, pa, &a);
        int rb = kvp_read(MOQR_PROPS_CODEC_VI64, p + ob, len - ob, pb, &b);
        if (ra < 0 || rb < 0 || ra != rb) return false;
        if (ra == 0) return true;
        if (a.type != b.type || a.size != b.size || a.type == PROPS_IMMUTABLE_TYPE)
            return false;
        if ((a.type & 1u) == 0 ? a.value != b.value
                               : a.length != b.length ||
                                 memcmp(a.bytes, b.bytes, (size_t)a.length) != 0)
            return false;
        pa = a.type; pb = b.type;
        oa += a.size; ob += b.size;
    }
}

moqr_props_conv_t moqr_core_internal_props_convert(
    uint8_t src_codec, uint8_t dst_codec, const uint8_t *in, size_t len,
    uint8_t *out, size_t cap, size_t *out_len)
{
    if (out_len == NULL || (in == NULL && len != 0)) return MOQR_PROPS_CONV_INVAL;
    if (!codec_valid(dst_codec) ||
        (src_codec != MOQR_PROPS_CODEC_UNKNOWN && !codec_valid(src_codec)))
        return MOQR_PROPS_CONV_INVAL;
    if (len == 0 || src_codec == dst_codec) {
        *out_len = len;
        return MOQR_PROPS_CONV_SAME;
    }
    if (src_codec == MOQR_PROPS_CODEC_UNKNOWN) return MOQR_PROPS_CONV_UNKNOWN;

    /* Two passes over the same reads: measure, then (with a buffer) write.
     * The measuring pass reads the whole block before judging the
     * destination, so a malformed source is MALFORMED whatever it would
     * have been converted to. */
    size_t need = 0;
    for (int pass = 0; pass < (out != NULL ? 2 : 1); pass++) {
        bool unsupported = false;
        if (pass == 1 && cap < need) {
            *out_len = need;
            return MOQR_PROPS_CONV_BUFFER;
        }
        size_t at = 0, w = 0;
        uint64_t prev = 0;
        for (;;) {
            props_kvp_t k;
            int r = kvp_read(src_codec, in + at, len - at, prev, &k);
            if (r < 0) return MOQR_PROPS_CONV_MALFORMED;
            if (r == 0) break;
            if (k.type == PROPS_IMMUTABLE_TYPE) {
                /* Its protected pairs are valid in the source codec first;
                 * then it is kept whole, only where its own header and those
                 * pairs read the same at the destination. */
                if (!block_valid(src_codec, k.bytes, (size_t)k.length))
                    return MOQR_PROPS_CONV_MALFORMED;
                props_kvp_t d;
                if (kvp_read(dst_codec, in + at, len - at, prev, &d) != 1 ||
                    d.type != k.type || d.size != k.size ||
                    d.length != k.length || !block_reads_alike(k.bytes, (size_t)k.length))
                    unsupported = true;
                if (pass == 1) memcpy(out + w, in + at, k.size);
                if (k.size > SIZE_MAX - w) return MOQR_PROPS_CONV_INVAL;
                w += k.size;
            } else {
                size_t dl = int_len(dst_codec, k.delta);
                size_t vl = (k.type & 1u) == 0 ? int_len(dst_codec, k.value)
                                               : int_len(dst_codec, k.length);
                if (dl == 0 || vl == 0) unsupported = true;
                if (pass == 1 && !unsupported) {
                    int_write(dst_codec, k.delta, out + w, cap - w);
                    if ((k.type & 1u) == 0) {
                        int_write(dst_codec, k.value, out + w + dl, cap - w - dl);
                    } else {
                        int_write(dst_codec, k.length, out + w + dl, cap - w - dl);
                        memcpy(out + w + dl + vl, k.bytes, (size_t)k.length);
                    }
                }
                size_t entry = dl + vl + ((k.type & 1u) ? (size_t)k.length : 0);
                if (entry > SIZE_MAX - w) return MOQR_PROPS_CONV_INVAL;
                w += entry;
            }
            prev = k.type;
            at += k.size;
        }
        if (unsupported) return MOQR_PROPS_CONV_UNSUPPORTED;
        need = w;
    }
    *out_len = need;
    return MOQR_PROPS_CONV_CONVERTED;
}
