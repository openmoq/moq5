/*
 * Track naming for MSF catalogs: UTF-8 validation, Track Namespace splitting
 * (MSF-02 5.2.2), Full Track Name comparison (11.2.2, 11.2.3) and the
 * open-addressing indexes that keep catalog-wide uniqueness and apply's
 * lookups O(n).
 */

#include <moq/msf.h>
#include "msf_internal.h"

#include <stdint.h>
#include <string.h>

#if defined(MOQ_MSF_TESTING)
/* Test-only compare counter (not MOQ_API; absent from the shipped library,
 * which never defines MOQ_MSF_TESTING). Proves the indexed lookups do O(n),
 * not O(n^2), identity compares -- see tests/test_msf_scaling.c. */
size_t moq_msf_test_identity_compares = 0;
#endif

/* -- UTF-8 ------------------------------------------------------------------ */

bool moq_msf_utf8_valid(const uint8_t *s, size_t len)
{
    size_t i = 0;
    while (i < len) {
        uint8_t c = s[i];
        if (c < 0x80) { i++; continue; }
        size_t n;
        uint32_t cp, min;
        if (c >= 0xC2 && c <= 0xDF)      { n = 1; cp = c & 0x1Fu; min = 0x80; }
        else if (c >= 0xE0 && c <= 0xEF) { n = 2; cp = c & 0x0Fu; min = 0x800; }
        else if (c >= 0xF0 && c <= 0xF4) { n = 3; cp = c & 0x07u; min = 0x10000; }
        else return false;           /* continuation, overlong lead, or > F4 */
        if (len - i - 1 < n) return false;
        for (size_t k = 1; k <= n; k++) {
            uint8_t cc = s[i + k];
            if ((cc & 0xC0u) != 0x80u) return false;
            cp = (cp << 6) | (cc & 0x3Fu);
        }
        /* No overlong forms, no surrogates, nothing past U+10FFFF: only
         * Unicode scalar values have a UTF-8 encoding (11.2.1). */
        if (cp < min || (cp >= 0xD800 && cp <= 0xDFFF) || cp > 0x10FFFF)
            return false;
        i += n + 1;
    }
    return true;
}

/* -- Track Namespace fields --------------------------------------------- */

moq_result_t moq_msf_namespace_split(moq_bytes_t ns, moq_bytes_t *out,
                                     size_t cap, size_t *out_count)
{
    if (!out_count) return MOQ_ERR_INVAL;
    *out_count = 0;
    if ((!out && cap > 0) || (ns.len > 0 && !ns.data)) return MOQ_ERR_INVAL;
    /* 5.2.2: 1..32 fields, each non-empty, so not empty and no leading,
     * trailing or doubled '/'. */
    if (ns.len == 0) return MOQ_ERR_PROTO;
    size_t n = 0, start = 0;
    for (size_t i = 0; i <= ns.len; i++) {
        if (i < ns.len && ns.data[i] != '/') continue;
        if (i == start) return MOQ_ERR_PROTO;            /* an empty field */
        if (n == MOQ_MSF_NAMESPACE_MAX_FIELDS) return MOQ_ERR_PROTO;
        n++;
        start = i + 1;
    }
    /* The fields are valid: write what fits. */
    size_t k = 0;
    start = 0;
    for (size_t i = 0; i <= ns.len && k < cap; i++) {
        if (i < ns.len && ns.data[i] != '/') continue;
        out[k++] = (moq_bytes_t){ ns.data + start, i - start };
        start = i + 1;
    }
    *out_count = n;
    return n > cap ? MOQ_ERR_BUFFER : MOQ_OK;
}

bool moq_msf_namespace_syntax_ok(moq_bytes_t ns)
{
    size_t n = 0;
    return moq_msf_namespace_split(ns, NULL, 0, &n) == MOQ_ERR_BUFFER;
}

/* Whether the '/'-separated string `s` names exactly the fields `parts`:
 * splitting is byte-exact and a field never holds '/', so this is field by
 * field equality, with no allocation. */
static bool ns_string_eq_parts(moq_bytes_t s, const moq_bytes_t *parts,
                               size_t count)
{
    size_t k = 0, start = 0;
    for (size_t i = 0; i <= s.len; i++) {
        if (i < s.len && s.data[i] != '/') continue;
        if (k == count) return false;
        size_t flen = i - start;
        if (parts[k].len != flen ||
            (flen && (!parts[k].data ||
                      memcmp(parts[k].data, s.data + start, flen) != 0)))
            return false;
        k++;
        start = i + 1;
    }
    return k == count;
}

static bool span_eq(moq_bytes_t a, moq_bytes_t b)
{
    return a.len == b.len &&
           (a.len == 0 || (a.data && b.data && memcmp(a.data, b.data, a.len) == 0));
}

/* The namespace a Full Track Name reference resolves to under `m`. */
typedef enum {
    NS_WRITTEN = 0,   /* MSF-01, or MSF-02 with "namespace": the string */
    NS_CATALOG,       /* MSF-02 without "namespace": the catalog's fields */
    NS_UNKNOWN,       /* MSF-02 without "namespace", catalog's unknown */
    NS_NONE,          /* MSF-01 without "namespace" */
} ns_class_t;

static ns_class_t ns_class(const msf_ident_mode_t *m, bool has_ns)
{
    if (has_ns) return NS_WRITTEN;
    if (!m->fields) return NS_NONE;
    return m->catalog_ns.count > 0 ? NS_CATALOG : NS_UNKNOWN;
}

bool moq_msf_ident_eq(const msf_ident_mode_t *m,
                      bool a_has, moq_bytes_t a_ns, moq_bytes_t a_name,
                      bool b_has, moq_bytes_t b_ns, moq_bytes_t b_name)
{
#if defined(MOQ_MSF_TESTING)
    moq_msf_test_identity_compares++;
#endif
    if (!span_eq(a_name, b_name)) return false;
    ns_class_t ca = ns_class(m, a_has), cb = ns_class(m, b_has);
    if (ca == NS_WRITTEN && cb == NS_WRITTEN) return span_eq(a_ns, b_ns);
    if (ca == cb) return true;      /* both the catalog's, unknown, or none */
    /* MSF-02: an explicit namespace equals the inherited one when it names
     * the same fields (11.2.2). */
    if (ca == NS_WRITTEN && cb == NS_CATALOG)
        return ns_string_eq_parts(a_ns, m->catalog_ns.parts, m->catalog_ns.count);
    if (ca == NS_CATALOG && cb == NS_WRITTEN)
        return ns_string_eq_parts(b_ns, m->catalog_ns.parts, m->catalog_ns.count);
    return false;
}

#define FNV_BASIS 1469598103934665603ULL
#define FNV_PRIME 1099511628211ULL

static uint64_t fnv_bytes(uint64_t h, const uint8_t *p, size_t n)
{
    for (size_t i = 0; i < n; i++) { h ^= p[i]; h *= FNV_PRIME; }
    return h;
}

/* A length hashed as eight octets whatever the width of size_t: widened
 * first, since shifting a 32-bit size_t by 32 or more is undefined. */
static uint64_t fnv_len(uint64_t h, size_t n)
{
    uint64_t v = (uint64_t)n;
    for (int i = 0; i < 8; i++) { h ^= (uint8_t)(v >> (8 * i)); h *= FNV_PRIME; }
    return h;
}

/* A hash consistent with moq_msf_ident_eq: in field mode it covers the
 * fields, length-prefixed, wherever they come from. */
static uint64_t ident_hash(const msf_ident_mode_t *m, bool has_ns,
                           moq_bytes_t ns, moq_bytes_t name)
{
    uint64_t h = FNV_BASIS;
    ns_class_t c = ns_class(m, has_ns);
    if (!m->fields) {
        uint8_t nb = has_ns ? 1u : 0u;
        h ^= nb; h *= FNV_PRIME;
        h = fnv_bytes(h, name.data, name.len);
        if (has_ns) h = fnv_bytes(h, ns.data, ns.len);
        return h;
    }
    uint8_t tag = c == NS_UNKNOWN ? 0u : 1u;
    h ^= tag; h *= FNV_PRIME;
    if (c == NS_WRITTEN) {
        size_t start = 0;
        for (size_t i = 0; i <= ns.len; i++) {
            if (i < ns.len && ns.data[i] != '/') continue;
            h = fnv_len(h, i - start);
            h = fnv_bytes(h, ns.data + start, i - start);
            start = i + 1;
        }
    } else if (c == NS_CATALOG) {
        for (size_t k = 0; k < m->catalog_ns.count; k++) {
            h = fnv_len(h, m->catalog_ns.parts[k].len);
            h = fnv_bytes(h, m->catalog_ns.parts[k].data, m->catalog_ns.parts[k].len);
        }
    }
    h ^= 0xFFu; h *= FNV_PRIME;
    h = fnv_len(h, name.len);
    return fnv_bytes(h, name.data, name.len);
}

/* -- Full Track Name index ---------------------------------------------------
 * An open-addressing hash set (linear probing, tombstones) mapping a track's
 * Full Track Name to its slot in a track array. Slots hold a track index
 * (>= 0), IX_EMPTY, or IX_TOMB (a removed entry whose probe chain must stay
 * intact). The load factor stays <= 0.5. */

#define IX_EMPTY ((long)-1)
#define IX_TOMB  ((long)-2)

static size_t pow2_cap(size_t n, bool *ok)
{
    size_t cap = 8;
    /* cap >= 2 * n, a power of two, wrap-guarded. */
    while (cap < n || (cap >> 1) < n) {
        if (cap > (SIZE_MAX >> 1)) { *ok = false; return 0; }
        cap <<= 1;
    }
    *ok = true;
    return cap;
}

moq_result_t moq_msf_index_init(msf_index_t *ix, size_t n,
                                const msf_ident_mode_t *mode,
                                const moq_alloc_t *alloc)
{
    bool ok;
    size_t cap = pow2_cap(n, &ok), bytes;
    if (!ok || !moq_msf_checked_array_bytes(cap, sizeof(long), &bytes))
        return MOQ_ERR_NOMEM;
    ix->slot = (long *)alloc->alloc(bytes, alloc->ctx);
    if (!ix->slot) return MOQ_ERR_NOMEM;
    for (size_t i = 0; i < cap; i++) ix->slot[i] = IX_EMPTY;
    ix->cap = cap;
    ix->mask = cap - 1;
    ix->mode = *mode;
    return MOQ_OK;
}

void moq_msf_index_free(msf_index_t *ix, const moq_alloc_t *alloc)
{
    if (ix->slot) alloc->free(ix->slot, ix->cap * sizeof(long), alloc->ctx);
    ix->slot = NULL;
    ix->cap = 0;
}

long moq_msf_index_find(const msf_index_t *ix, const moq_msf_track_t *tracks,
                        bool has_ns, moq_bytes_t ns, moq_bytes_t name)
{
    size_t i = (size_t)ident_hash(&ix->mode, has_ns, ns, name) & ix->mask;
    for (size_t p = 0; p <= ix->mask; p++) {
        long s = ix->slot[i];
        if (s == IX_EMPTY) return -1;
        if (s >= 0 && moq_msf_ident_eq(&ix->mode, has_ns, ns, name,
                                       tracks[s].has_namespace,
                                       tracks[s].namespace_, tracks[s].name))
            return s;
        i = (i + 1) & ix->mask;
    }
    return -1;
}

void moq_msf_index_insert(msf_index_t *ix, const moq_msf_track_t *tracks,
                          long ti)
{
    const moq_msf_track_t *t = &tracks[ti];
    size_t i = (size_t)ident_hash(&ix->mode, t->has_namespace, t->namespace_,
                                  t->name) & ix->mask;
    for (size_t p = 0; p <= ix->mask; p++) {
        long s = ix->slot[i];
        if (s == IX_EMPTY || s == IX_TOMB) { ix->slot[i] = ti; return; }
        i = (i + 1) & ix->mask;
    }
    /* Unreachable: the load factor guarantees a free slot. */
}

void moq_msf_index_remove(msf_index_t *ix, const moq_msf_track_t *tracks,
                          bool has_ns, moq_bytes_t ns, moq_bytes_t name)
{
    size_t i = (size_t)ident_hash(&ix->mode, has_ns, ns, name) & ix->mask;
    for (size_t p = 0; p <= ix->mask; p++) {
        long s = ix->slot[i];
        if (s == IX_EMPTY) return;
        if (s >= 0 && moq_msf_ident_eq(&ix->mode, has_ns, ns, name,
                                       tracks[s].has_namespace,
                                       tracks[s].namespace_, tracks[s].name)) {
            ix->slot[i] = IX_TOMB;
            return;
        }
        i = (i + 1) & ix->mask;
    }
}

/* -- Byte-span counts --------------------------------------------------------
 * The same open-addressing scheme keyed on one byte span, counting how many
 * times each key was added: the initDataList-id and refID uniqueness checks
 * and "depends" name resolution. Keys are borrowed for the index's life. An
 * empty slot has len == SIZE_MAX, which no real key uses, so a zero-length
 * key is still storable. */

moq_result_t moq_msf_span_index_init(msf_span_index_t *ix, size_t n,
                                     const moq_alloc_t *alloc)
{
    bool ok;
    size_t cap = pow2_cap(n, &ok), bytes;
    if (!ok || !moq_msf_checked_array_bytes(cap, sizeof(msf_span_slot_t), &bytes))
        return MOQ_ERR_NOMEM;
    ix->slot = (msf_span_slot_t *)alloc->alloc(bytes, alloc->ctx);
    if (!ix->slot) return MOQ_ERR_NOMEM;
    for (size_t i = 0; i < cap; i++) {
        ix->slot[i].key = (moq_bytes_t){ NULL, SIZE_MAX };
        ix->slot[i].count = 0;
    }
    ix->cap = cap;
    ix->mask = cap - 1;
    return MOQ_OK;
}

void moq_msf_span_index_free(msf_span_index_t *ix, const moq_alloc_t *alloc)
{
    if (ix->slot)
        alloc->free(ix->slot, ix->cap * sizeof(msf_span_slot_t), alloc->ctx);
    ix->slot = NULL;
    ix->cap = 0;
}

static msf_span_slot_t *span_slot(const msf_span_index_t *ix, moq_bytes_t k,
                                  bool *found)
{
    size_t i = (size_t)fnv_bytes(FNV_BASIS, k.data, k.len) & ix->mask;
    for (size_t p = 0; p <= ix->mask; p++) {
        msf_span_slot_t *s = &ix->slot[i];
        if (s->key.len == SIZE_MAX) { *found = false; return s; }
#if defined(MOQ_MSF_TESTING)
        moq_msf_test_identity_compares++;
#endif
        if (span_eq(s->key, k)) { *found = true; return s; }
        i = (i + 1) & ix->mask;
    }
    *found = false;
    return NULL;   /* unreachable: the load factor guarantees a free slot */
}

size_t moq_msf_span_index_add(msf_span_index_t *ix, moq_bytes_t k)
{
    bool found;
    msf_span_slot_t *s = span_slot(ix, k, &found);
    if (!s) return 0;
    if (!found) { s->key = k; s->count = 0; }
    return ++s->count;
}

size_t moq_msf_span_index_count(const msf_span_index_t *ix, moq_bytes_t k)
{
    bool found;
    const msf_span_slot_t *s = span_slot(ix, k, &found);
    return found ? s->count : 0;
}

/* -- Matching a catalog track against MOQT names --------------------------- */

static bool span_usable(moq_bytes_t b)
{
    return b.len == 0 || b.data != NULL;
}

/* MOQT bytes a catalog can name: a usable span holding UTF-8 (11.2.3). */
static bool moqt_naming_ok(moq_bytes_t b)
{
    return span_usable(b) && moq_msf_utf8_valid(b.data, b.len);
}

bool moq_msf_track_matches(const moq_msf_track_t *track,
                           moq_namespace_t catalog_namespace,
                           moq_namespace_t ns, moq_bytes_t name)
{
    if (!track || (ns.count > 0 && !ns.parts)) return false;
    /* 11.2.3: a name or namespace field received from MOQT that is not UTF-8
     * can never match a catalog entry, whatever bytes the entry holds. */
    if (!moqt_naming_ok(name)) return false;
    for (size_t k = 0; k < ns.count; k++)
        if (!moqt_naming_ok(ns.parts[k])) return false;
    if (!span_usable(track->name) || !span_eq(track->name, name)) return false;
    if (track->has_namespace) {
        if (!span_usable(track->namespace_)) return false;
        return ns_string_eq_parts(track->namespace_, ns.parts, ns.count);
    }
    /* 5.2.2: no "namespace" inherits the catalog's; unknown matches nothing. */
    if (catalog_namespace.count == 0 || !catalog_namespace.parts ||
        catalog_namespace.count != ns.count)
        return false;
    for (size_t k = 0; k < ns.count; k++)
        if (!span_usable(catalog_namespace.parts[k]) ||
            !span_eq(catalog_namespace.parts[k], ns.parts[k]))
            return false;
    return true;
}
