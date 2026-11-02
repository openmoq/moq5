#include "shards_internal.h"
#include "placement_internal.h"

/* -- placement: rendezvous (highest-random-weight) hash --------------------- */

uint64_t
r_splitmix64(uint64_t x)
{
    x += 0x9E3779B97F4A7C15ull;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
    return x ^ (x >> 31);
}


/* HRW score of a key hash for a candidate shard. Pure. */
static uint64_t
r_hrw_score(uint64_t hash, uint32_t shard)
{
    return r_splitmix64(hash ^ ((uint64_t)shard * 0x9E3779B97F4A7C15ull));
}


/* The default policy: score each shard and take the maximum (ties to the lowest
 * index). Pure in (key, state) — no clocks, counters, or globals — so the same
 * key and state always pick the same owner, which is what lets placement coexist
 * with seed-replay determinism. */
moqr_owner_t
r_rendezvous_place(const moqr_place_key_t *key,
                   const moqr_placement_state_t *state)
{
    moqr_owner_t owner = { 0, 0 };
    uint32_t k = state->shard_count == 0 ? 1u : (uint32_t)state->shard_count;
    uint64_t best_score = 0;
    for (uint32_t i = 0; i < k; i++) {
        uint64_t score = r_hrw_score(key->hash, i);
        if (i == 0 || score > best_score) {
            best_score = score;
            owner.shard = i;
        }
    }
    return owner;
}


/* HRW winner over a live candidate set (bit i = shard i). -1 if empty. */
int32_t
r_winner_of(uint64_t candidates, uint64_t hash)
{
    int32_t best = -1;
    uint64_t best_score = 0;
    for (uint32_t i = 0; i < 64u; i++) {
        if (((candidates >> i) & 1u) == 0) {
            continue;
        }
        uint64_t score = r_hrw_score(hash, i);
        if (best < 0 || score > best_score) {
            best_score = score;
            best = (int32_t)i;
        }
    }
    return best;
}


/* -- canonical namespace key ------------------------------------------------ */
/* Layout: [u32 count][u32 len_0..len_{c-1}][part bytes...], little-endian. The
 * journal keys and mailbox messages both use this single encoding, and it
 * reconstructs a moqr_ns_t for the mirror announce. */

uint32_t
rd32(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) |
           ((uint32_t)p[3] << 24);
}


void
wr32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
}


uint8_t *
canon_build(const moq_bytes_t *parts, uint32_t count, uint32_t *out_len,
            const moq_alloc_t *a)
{
    if (count > R_NS_MAX_PARTS) {
        return NULL;
    }
    uint32_t len = 4u + 4u * count;
    for (uint32_t i = 0; i < count; i++) {
        len += (uint32_t)parts[i].len;
    }
    uint8_t *buf = sh_alloc(a, len);
    if (buf == NULL) {
        return NULL;
    }
    wr32(buf, count);
    uint32_t off = 4u + 4u * count;
    for (uint32_t i = 0; i < count; i++) {
        uint32_t l = (uint32_t)parts[i].len;
        wr32(buf + 4u + 4u * i, l);
        if (l != 0) {
            memcpy(buf + off, parts[i].data, l);
        }
        off += l;
    }
    *out_len = len;
    return buf;
}


uint8_t *
canon_dup(const uint8_t *canon, uint32_t len, const moq_alloc_t *a)
{
    uint8_t *b = sh_alloc(a, len);
    if (b != NULL) {
        memcpy(b, canon, len);
    }
    return b;
}


uint64_t
canon_hash(const uint8_t *canon, uint32_t len)
{
    uint64_t h = 0xCBF29CE484222325ull;
    for (uint32_t i = 0; i < len; i++) {
        h ^= canon[i];
        h *= 0x100000001B3ull;
    }
    return h;
}


/* Total order over canonical keys (lexicographic bytes, shorter first on a
 * shared prefix). The dirty-reconcile and outbound phases walk in this order. */
int
canon_cmp(const uint8_t *a, uint32_t alen, const uint8_t *b, uint32_t blen)
{
    uint32_t n = alen < blen ? alen : blen;
    int c = memcmp(a, b, n);
    if (c != 0) {
        return c;
    }
    return alen < blen ? -1 : (alen > blen ? 1 : 0);
}


moqr_ns_t
canon_to_ns(const uint8_t *canon, moq_bytes_t *parts)
{
    uint32_t count = rd32(canon);
    uint32_t off = 4u + 4u * count;
    for (uint32_t i = 0; i < count; i++) {
        uint32_t l = rd32(canon + 4u + 4u * i);
        parts[i].data = (l != 0) ? canon + off : NULL;
        parts[i].len = l;
        off += l;
    }
    moqr_ns_t ns = { parts, count };
    return ns;
}


/* -- the demand channel (FIFO; sibling to — never part of — the mailbox) ----- */

/* Canonical ns+name key: the mailbox canon layout with a trailing
 * [u32 name_len][name bytes] segment, so one owned buffer carries the full
 * track identity a DEMAND needs. */
uint8_t *
canon2_build(const moq_bytes_t *parts, uint32_t count, moq_bytes_t name,
             uint32_t *out_len, const moq_alloc_t *a)
{
    if (count > R_NS_MAX_PARTS) {
        return NULL;
    }
    uint32_t len = 4u + 4u * count;
    for (uint32_t i = 0; i < count; i++) {
        len += (uint32_t)parts[i].len;
    }
    uint32_t ns_len = len;
    len += 4u + (uint32_t)name.len;
    uint8_t *buf = sh_alloc(a, len);
    if (buf == NULL) {
        return NULL;
    }
    wr32(buf, count);
    uint32_t off = 4u + 4u * count;
    for (uint32_t i = 0; i < count; i++) {
        uint32_t l = (uint32_t)parts[i].len;
        wr32(buf + 4u + 4u * i, l);
        if (l != 0) {
            memcpy(buf + off, parts[i].data, l);
        }
        off += l;
    }
    wr32(buf + ns_len, (uint32_t)name.len);
    if (name.len != 0) {
        memcpy(buf + ns_len + 4u, name.data, name.len);
    }
    *out_len = len;
    return buf;
}


/* Length of the ns prefix inside a canon2 key — the exact bytes canon_build
 * would produce for the namespace alone, so the journal (keyed on that
 * layout) can be searched with a canon2 key's prefix. */
uint32_t
canon2_ns_len(const uint8_t *canon)
{
    uint32_t count = rd32(canon);
    uint32_t len = 4u + 4u * count;
    for (uint32_t i = 0; i < count; i++) {
        len += rd32(canon + 4u + 4u * i);
    }
    return len;
}


/* Decode a canon2 key back into a borrowed {ns, name} view (valid while the
 * key lives). */
moqr_ns_t
canon2_decode(const uint8_t *canon, moq_bytes_t *parts, moq_bytes_t *name)
{
    moqr_ns_t ns = canon_to_ns(canon, parts);
    uint32_t off = canon2_ns_len(canon);
    uint32_t nlen = rd32(canon + off);
    name->data = (nlen != 0) ? canon + off + 4u : NULL;
    name->len = nlen;
    return ns;
}
