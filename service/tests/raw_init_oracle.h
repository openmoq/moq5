/*
 * The record oracle for an init-bearing RAW/LOC track, shared by the RED rows
 * and their controls so that both judge the SAME function.
 *
 * A track's record is resolved the one way the sender's ruled producer
 * contract declares: through its initRef into the root Initialization Data
 * List (MSF-01 §5.1.7, §5.2.13), then base64-decoded, then compared byte for
 * byte. A per-track inline initData -- present with bytes, or present and
 * empty -- is a legacy shape this contract does not produce, and it resolves
 * to nothing here. Our own receiver tolerates it; that tolerance is
 * compatibility and is not consulted.
 *
 * Header-only, static inline (a TU may use a subset), no allocation kept:
 * every decode is released before returning.
 */
#ifndef RAW_INIT_ORACLE_H
#define RAW_INIT_ORACLE_H

#include <moq/msf.h>
#include <moq/rcbuf.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

static inline const moq_msf_track_t *raw_init_track(const moq_msf_catalog_t *c,
                                             const char *name)
{
    size_t nl = strlen(name);
    for (size_t i = 0; i < c->track_count; i++)
        if (c->tracks[i].name.len == nl &&
            memcmp(c->tracks[i].name.data, name, nl) == 0)
            return &c->tracks[i];
    return NULL;
}

/* Why a resolution failed, so a control can require the NAMED reason. */
typedef enum {
    RAW_INIT_OK = 0,
    RAW_INIT_NO_TRACK,       /* no track of that name */
    RAW_INIT_LEGACY_INLINE,  /* the track carries a per-track initData field */
    RAW_INIT_NO_REF,         /* the track names no list entry */
    RAW_INIT_DANGLING_REF,   /* the named entry is not in the list */
    RAW_INIT_EMPTY_ENTRY,    /* the entry carries no data */
    RAW_INIT_UNDECODABLE,    /* the entry's data is not base64 */
    RAW_INIT_WRONG_BYTES,    /* the decoded record differs from the declared one */
} raw_init_verdict_t;

/*
 * Resolve `name`'s record and compare it to `want`. The verdict names the
 * first invariant that failed; RAW_INIT_OK means every one held.
 */
static inline raw_init_verdict_t raw_init_resolve(const moq_msf_catalog_t *c,
                                           const char *name,
                                           const uint8_t *want, size_t want_len)
{
    const moq_msf_track_t *t = raw_init_track(c, name);
    if (!t) return RAW_INIT_NO_TRACK;
    if (t->has_init_data) return RAW_INIT_LEGACY_INLINE;   /* even when empty */
    if (!t->has_init_ref || t->init_ref.len == 0) return RAW_INIT_NO_REF;
    const moq_msf_init_data_entry_t *e =
        moq_msf_catalog_find_init_data(c, t->init_ref);
    if (!e) return RAW_INIT_DANGLING_REF;
    if (e->data.len == 0) return RAW_INIT_EMPTY_ENTRY;
    moq_rcbuf_t *decoded = NULL;
    if (moq_msf_decode_init_data(moq_alloc_default(), e->data, &decoded) != MOQ_OK
            || !decoded)
        return RAW_INIT_UNDECODABLE;
    bool same = moq_rcbuf_len(decoded) == want_len &&
                memcmp(moq_rcbuf_data(decoded), want, want_len) == 0;
    moq_rcbuf_decref(decoded);
    return same ? RAW_INIT_OK : RAW_INIT_WRONG_BYTES;
}

/* The one entry type MSF-01 §5.1.7 defines, on every entry. */
static inline bool raw_init_list_is_inline(const moq_msf_catalog_t *c)
{
    for (size_t i = 0; i < c->init_data_count; i++)
        if (c->init_data_list[i].type.len != 6 ||
            memcmp(c->init_data_list[i].type.data, "inline", 6) != 0)
            return false;
    return true;
}

/* No track carries the legacy per-track field, present with bytes or empty. */
static inline bool raw_init_no_legacy_field(const moq_msf_catalog_t *c)
{
    for (size_t i = 0; i < c->track_count; i++)
        if (c->tracks[i].has_init_data) return false;
    return true;
}

/*
 * No leftovers: every list entry is referenced by at least one track, and
 * every track that references an entry names one that exists. Multiplicity is
 * the caller's to declare; this rules out an entry nothing uses and a
 * reference nothing satisfies.
 */
static inline bool raw_init_list_is_exactly_used(const moq_msf_catalog_t *c)
{
    for (size_t i = 0; i < c->init_data_count; i++) {
        const moq_msf_init_data_entry_t *e = &c->init_data_list[i];
        bool used = false;
        for (size_t j = 0; j < c->track_count && !used; j++)
            used = c->tracks[j].has_init_ref &&
                   c->tracks[j].init_ref.len == e->id.len &&
                   memcmp(c->tracks[j].init_ref.data, e->id.data, e->id.len) == 0;
        if (!used) return false;
    }
    for (size_t j = 0; j < c->track_count; j++)
        if (c->tracks[j].has_init_ref &&
            moq_msf_catalog_find_init_data(c, c->tracks[j].init_ref) == NULL)
            return false;
    return true;
}

/* Two tracks' references are the same identity. */
static inline bool raw_init_same_ref(const moq_msf_track_t *a, const moq_msf_track_t *b)
{
    return a && b && a->has_init_ref && b->has_init_ref &&
           a->init_ref.len == b->init_ref.len &&
           memcmp(a->init_ref.data, b->init_ref.data, a->init_ref.len) == 0;
}

/* A reference identity, copied out so it can be compared across generations. */
typedef struct { uint8_t id[64]; size_t len; bool ok; } raw_init_ref_t;

static inline raw_init_ref_t raw_init_ref_of(const moq_msf_catalog_t *c, const char *name)
{
    raw_init_ref_t r;
    memset(&r, 0, sizeof(r));
    const moq_msf_track_t *t = raw_init_track(c, name);
    if (t && t->has_init_ref && t->init_ref.len > 0 &&
        t->init_ref.len <= sizeof(r.id)) {
        memcpy(r.id, t->init_ref.data, t->init_ref.len);
        r.len = t->init_ref.len;
        r.ok = true;
    }
    return r;
}

static inline bool raw_init_ref_eq(raw_init_ref_t a, raw_init_ref_t b)
{
    return a.ok && b.ok && a.len == b.len && memcmp(a.id, b.id, a.len) == 0;
}

#endif /* RAW_INIT_ORACLE_H */
