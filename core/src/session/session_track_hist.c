#include "session_internal.h"
#include "../internal/validate.h"
#include <moq/wire.h>

/* -- Per-track largest-location history registry ------------------- */
/*
 * The registry records the largest Object Location published or received on
 * each track (§5.1.2 "Largest Object"), keyed by the canonical full-track-name
 * blob. It backs the mandatory LARGEST_OBJECT parameter on
 * SUBSCRIBE_OK / PUBLISH / REQUEST_OK and relative-filter resolution.
 *
 * Storage is a flat pool of `th_cap` slots, separately allocated from the
 * session block so the cap does not perturb the block layout. Lookup is a
 * linear scan over occupied slots -- the pool is small (defaults to
 * subs+publishes+track-statuses) and each request entry touches its record
 * at establishment and teardown, not on the object hot path (the facade holds
 * a direct record pointer for per-object merges).
 */

/*
 * Canonical full-track-name key: [ns.count u8][ (u16 len, bytes) per part ]
 * [u16 name len, name bytes]. Identical byte layout to build_track_id() in
 * session_subscribe.c so keys reserved at subscribe/publish/track-status
 * establishment match keys built here and by the public note API.
 * Returns an owned blob (caller frees via s->alloc) and sets *out_len, or
 * NULL on OOM.
 */
uint8_t *moq_build_track_key(moq_session_t *s,
                             const moq_namespace_t *ns,
                             moq_bytes_t name,
                             size_t *out_len)
{
    size_t total = 1;
    for (size_t i = 0; i < ns->count; i++)
        total += 2 + ns->parts[i].len;
    total += 2 + name.len;
    *out_len = total;

    uint8_t *buf = (uint8_t *)s->alloc.alloc(total, s->alloc.ctx);
    if (!buf) return NULL;

    size_t off = 0;
    buf[off++] = (uint8_t)ns->count;
    for (size_t i = 0; i < ns->count; i++) {
        uint16_t flen = (uint16_t)ns->parts[i].len;
        buf[off++] = (uint8_t)(flen >> 8);
        buf[off++] = (uint8_t)(flen & 0xFF);
        if (flen > 0)
            memcpy(buf + off, ns->parts[i].data, flen);
        off += flen;
    }
    uint16_t nlen = (uint16_t)name.len;
    buf[off++] = (uint8_t)(nlen >> 8);
    buf[off++] = (uint8_t)(nlen & 0xFF);
    if (name.len > 0 && name.data)
        memcpy(buf + off, name.data, name.len);

    return buf;
}

moq_track_hist_t *track_hist_find(moq_session_t *s,
                                  const uint8_t *key, size_t key_len)
{
    if (!s->track_hist) return NULL;
    for (size_t i = 0; i < s->th_cap; i++) {
        moq_track_hist_t *r = &s->track_hist[i];
        if (r->in_use && r->key_len == key_len &&
            (key_len == 0 || memcmp(r->key, key, key_len) == 0))
            return r;
    }
    return NULL;
}

/*
 * Compare a decoded identity against a stored canonical key in place.
 * Every field length is validated against the remaining bytes before it
 * is read, and the total must be consumed exactly, so a truncated or
 * malformed stored key fails to match instead of over-reading.
 */
bool moq_track_key_matches(const uint8_t *key, size_t key_len,
                           const moq_namespace_t *ns, moq_bytes_t name)
{
    /* Only the canonical encoding matches: the empty identity is three
     * bytes ([count 0][u16 name len 0]), never a NULL/zero-length blob. */
    if (!key || key_len < 1) return false;

    size_t off = 0;
    if (key[off++] != (uint8_t)ns->count) return false;
    /* A count that cannot round-trip the stored u8 never matches. */
    if (ns->count > 0xFF) return false;

    for (size_t i = 0; i < ns->count; i++) {
        if (key_len - off < 2) return false;
        size_t flen = ((size_t)key[off] << 8) | (size_t)key[off + 1];
        off += 2;
        if (flen != ns->parts[i].len) return false;
        if (key_len - off < flen) return false;
        if (flen > 0 &&
            memcmp(key + off, ns->parts[i].data, flen) != 0) return false;
        off += flen;
    }

    if (key_len - off < 2) return false;
    size_t nlen = ((size_t)key[off] << 8) | (size_t)key[off + 1];
    off += 2;
    if (nlen != name.len) return false;
    if (key_len - off < nlen) return false;
    if (nlen > 0 && memcmp(key + off, name.data, nlen) != 0) return false;
    off += nlen;

    return off == key_len;
}

moq_track_hist_t *track_hist_find_id(moq_session_t *s,
                                     const moq_namespace_t *ns,
                                     moq_bytes_t name)
{
    if (!s->track_hist) return NULL;
    for (size_t i = 0; i < s->th_cap; i++) {
        moq_track_hist_t *r = &s->track_hist[i];
        if (r->in_use && moq_track_key_matches(r->key, r->key_len, ns, name))
            return r;
    }
    return NULL;
}

moq_track_hist_sel_t moq_track_hist_select(moq_session_t *s,
                                           const moq_namespace_t *ns,
                                           moq_bytes_t name)
{
    if (track_hist_find_id(s, ns, name)) return MOQ_TH_SEL_EXISTING;
    if (!s->track_hist) return MOQ_TH_SEL_FULL;
    for (size_t i = 0; i < s->th_cap; i++)
        if (!s->track_hist[i].in_use) return MOQ_TH_SEL_FREE_SLOT;
    return MOQ_TH_SEL_FULL;
}

moq_result_t track_hist_reserve_selected(moq_session_t *s,
                                         const moq_namespace_t *ns,
                                         moq_bytes_t name,
                                         moq_track_hist_t **out)
{
    *out = NULL;

    moq_track_hist_t *rec = track_hist_find_id(s, ns, name);
    if (rec) {
        rec->refs++;
        *out = rec;
        return MOQ_OK;
    }

    moq_track_hist_t *slot = NULL;
    if (s->track_hist) {
        for (size_t i = 0; i < s->th_cap; i++) {
            if (!s->track_hist[i].in_use) { slot = &s->track_hist[i]; break; }
        }
    }
    /* Callers select first and route MOQ_TH_SEL_FULL to its own funded
     * REQUEST_ERROR path, so reaching here without a slot means the
     * registry filled between selection and execution -- impossible on a
     * single-threaded advancing call, and reported distinctly rather than
     * as an allocation failure. */
    if (!slot) return MOQ_ERR_INTERNAL;

    size_t key_len = 0;
    uint8_t *kcopy = moq_build_track_key(s, ns, name, &key_len);
    if (!kcopy && key_len > 0) return MOQ_ERR_NOMEM;   /* nothing mutated */

    slot->in_use = true;
    slot->has_largest = false;
    slot->refs = 1;
    slot->key = kcopy;
    slot->key_len = key_len;
    slot->largest_group = 0;
    slot->largest_object = 0;
    *out = slot;
    return MOQ_OK;
}

moq_track_hist_t *track_hist_reserve(moq_session_t *s,
                                     const uint8_t *key, size_t key_len)
{
    moq_track_hist_t *rec = track_hist_find(s, key, key_len);
    if (rec) {
        rec->refs++;
        return rec;
    }
    if (!s->track_hist) return NULL;

    /* Find a free slot; the pool is hard-bounded (no eviction). */
    moq_track_hist_t *slot = NULL;
    for (size_t i = 0; i < s->th_cap; i++) {
        if (!s->track_hist[i].in_use) { slot = &s->track_hist[i]; break; }
    }
    if (!slot) return NULL;               /* registry full */

    uint8_t *kcopy = NULL;
    if (key_len > 0) {
        kcopy = (uint8_t *)s->alloc.alloc(key_len, s->alloc.ctx);
        if (!kcopy) return NULL;          /* OOM: no record created */
        memcpy(kcopy, key, key_len);
    }
    slot->in_use = true;
    slot->has_largest = false;
    slot->refs = 1;
    slot->key = kcopy;
    slot->key_len = key_len;
    slot->largest_group = 0;
    slot->largest_object = 0;
    return slot;
}

void track_hist_release(moq_session_t *s, moq_track_hist_t *rec)
{
    if (!rec) return;
    if (rec->refs > 0) rec->refs--;
    /* Empty (unobserved) reservations are reclaimed once unreferenced; an
     * observed record is pinned to session end regardless of refs. */
    if (rec->refs == 0 && !rec->has_largest) {
        if (rec->key) s->alloc.free(rec->key, rec->key_len, s->alloc.ctx);
        rec->key = NULL;
        rec->key_len = 0;
        rec->in_use = false;
    }
}

void track_hist_merge(moq_track_hist_t *rec,
                      uint64_t group_id, uint64_t object_id)
{
    if (!rec) return;
    /* Monotonic max in Location order; first observation always sets. */
    if (!rec->has_largest ||
        group_id > rec->largest_group ||
        (group_id == rec->largest_group && object_id > rec->largest_object)) {
        rec->largest_group = group_id;
        rec->largest_object = object_id;
        rec->has_largest = true;
    }
}

void track_hist_destroy(moq_session_t *s)
{
    if (!s->track_hist) return;
    for (size_t i = 0; i < s->th_cap; i++) {
        moq_track_hist_t *r = &s->track_hist[i];
        if (r->in_use && r->key)
            s->alloc.free(r->key, r->key_len, s->alloc.ctx);
    }
    s->alloc.free(s->track_hist, s->th_cap * sizeof(*s->track_hist),
                  s->alloc.ctx);
    s->track_hist = NULL;
    s->th_cap = 0;
}

/* -- Filter resolution ----------- */

bool moq_loc_successor(uint64_t group, uint64_t object, uint64_t ceiling,
                       uint64_t *out_group, uint64_t *out_object)
{
    if (object < ceiling) {
        *out_group = group;
        *out_object = object + 1;
        return false;
    }
    /* object at ceiling: carry to the next group's object 0. */
    if (group < ceiling) {
        *out_group = group + 1;
        *out_object = 0;
        return false;
    }
    /* Both at ceiling: no representable successor. */
    return true;
}

void moq_resolve_filter_window(moq_subscribe_filter_t filter,
                               uint64_t raw_start_group,
                               uint64_t raw_start_object,
                               uint64_t raw_end_group,
                               bool has_snap,
                               uint64_t snap_group, uint64_t snap_object,
                               uint64_t ceiling,
                               moq_resolved_window_t *out)
{
    memset(out, 0, sizeof(*out));
    out->has_window = true;
    out->filter = filter;

    switch (filter) {
    case MOQ_SUBSCRIBE_FILTER_NEXT_GROUP:
        if (!has_snap) {
            /* No observed/seeded largest: open window from the origin. */
            out->start_group = 0;
            out->start_object = 0;
        } else {
            /* Successor of {LG, ceiling} == {LG+1, 0}. */
            out->unsatisfiable = moq_loc_successor(
                snap_group, ceiling, ceiling,
                &out->start_group, &out->start_object);
        }
        break;
    case MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT:
        if (!has_snap) {
            out->start_group = 0;
            out->start_object = 0;
        } else {
            out->unsatisfiable = moq_loc_successor(
                snap_group, snap_object, ceiling,
                &out->start_group, &out->start_object);
        }
        break;
    case MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START:
        out->start_group = raw_start_group;
        out->start_object = raw_start_object;
        break;
    case MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE:
        out->start_group = raw_start_group;
        out->start_object = raw_start_object;
        out->has_end = true;
        out->end_group = raw_end_group;
        break;
    case MOQ_SUBSCRIBE_FILTER_NONE:
    default:
        /* No filter constraint: open window from the origin. */
        out->start_group = 0;
        out->start_object = 0;
        break;
    }
}

/* -- Public API ----------------------------------------------------- */

moq_result_t moq_session_note_object_published(
    moq_session_t *s,
    const moq_namespace_t *ns,
    moq_bytes_t track_name,
    uint64_t group_id,
    uint64_t object_id)
{
    if (!s || !ns) return MOQ_ERR_INVAL;
    /* Bound against the PROFILE's Location ceiling (design §4): draft-16 QUIC
     * varint (2^62-1), draft-18 vi64 (full uint64). A valid d18 location above
     * 2^62-1 must not be rejected. */
    uint64_t loc_max = s->profile->location_varint_max;
    if (group_id > loc_max || object_id > loc_max)
        return MOQ_ERR_INVAL;
    /* Reject malformed track names before touching any state: a NULL/empty
     * namespace, a NULL component with nonzero length, or a NULL name with
     * nonzero length would otherwise be dereferenced / memcpy'd while building
     * the key. Same gate the request-initiating APIs use. */
    if (moq_validate_full_track_name(ns, track_name) < 0) return MOQ_ERR_INVAL;
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

    size_t key_len = 0;
    uint8_t *key = moq_build_track_key(s, ns, track_name, &key_len);
    if (!key && key_len > 0) return MOQ_ERR_NOMEM;

    moq_result_t rc = MOQ_OK;
    moq_track_hist_t *rec = track_hist_find(s, key, key_len);
    if (rec) {
        track_hist_merge(rec, group_id, object_id);
    } else {
        /* Public path may allocate/create a record; a full registry fails
         * without recording anything. Reserve then immediately release the
         * transient reference so an unobserved-but-created record does not
         * pin a slot; the merge sets has_largest, which pins it. */
        rec = track_hist_reserve(s, key, key_len);
        if (!rec) {
            rc = MOQ_ERR_NOMEM;
        } else {
            track_hist_merge(rec, group_id, object_id);
            track_hist_release(s, rec);   /* refs 1->0; has_largest pins it */
        }
    }

    if (key) s->alloc.free(key, key_len, s->alloc.ctx);
    return rc;
}
