#ifndef MOQ_FILL_IDENTITY_INTERNAL_H
#define MOQ_FILL_IDENTITY_INTERNAL_H

#include <stdbool.h>
#include <stdint.h>

/* Facade fill tokens (moq_sub_fill_t, moq_pub_fill_t): a 15-bit instance
 * tag above a 48-bit sequence, both private to the facade instance that
 * issued them. The tag is hashed at create from the instance address, the
 * session's tag, the count of facades the session has created and the
 * facade kind, odd so a token is never zero; the sequence starts at 1 and
 * never wraps. Resolution checks the tag, then the instance's own table, so
 * a token from another instance, or from a destroyed one, is refused with
 * the odds of a 15-bit tag match, as core handles are. No shared state. */
#define FACADE_FILL_SEQ_BITS 48
#define FACADE_FILL_SEQ_MAX ((UINT64_C(1) << FACADE_FILL_SEQ_BITS) - 1)
#define FACADE_FILL_KIND_SUBSCRIBER 1u
#define FACADE_FILL_KIND_PUBLISHER  2u

static inline uint16_t facade_fill_tag(const void *instance, uint16_t session_tag,
                                       uint16_t serial, unsigned kind)
{
    /* Every input goes in before the multiply, which spreads it over the
     * word, so each one reaches the low bits the tag is taken from. */
    uint64_t h = (uint64_t)(uintptr_t)instance;
    h ^= (uint64_t)session_tag << 16;
    h ^= (uint64_t)serial << 32;
    h ^= (uint64_t)kind << 48;
    h ^= h >> 16;
    h ^= h >> 32;
    h *= 0x9E3779B97F4A7C15ULL;
    h ^= h >> 33;
    return (uint16_t)((h & 0x7FFF) | 1);
}

/* Issue the next sequence of an instance; false, with nothing changed, once
 * the space is exhausted. */
static inline bool facade_fill_next(uint64_t *last_issued, uint64_t *out)
{
    if (*last_issued >= FACADE_FILL_SEQ_MAX) return false;
    *out = ++*last_issued;
    return true;
}

static inline uint64_t facade_fill_pack(uint16_t tag, uint64_t seq)
{
    return ((uint64_t)tag << FACADE_FILL_SEQ_BITS) | (seq & FACADE_FILL_SEQ_MAX);
}

static inline uint16_t facade_fill_tag_of(uint64_t token)
{
    return (uint16_t)(token >> FACADE_FILL_SEQ_BITS);
}

static inline uint64_t facade_fill_seq_of(uint64_t token)
{
    return token & FACADE_FILL_SEQ_MAX;
}

#endif /* MOQ_FILL_IDENTITY_INTERNAL_H */
