/*
 * The inbound semantic check of an independent MSF catalog: Full Track Name,
 * initDataList-id and refID uniqueness, and MSF-02 "depends" resolution.
 */

#include <moq/msf.h>
#include "msf_internal.h"

#include <string.h>

static moq_result_t check_fail(moq_msf_catalog_error_t *reason,
                               moq_msf_catalog_error_t why)
{
    if (reason) *reason = why;
    return MOQ_ERR_PROTO;
}

/* An identity span is safe to hash and compare only if it is not the
 * reserved empty-slot sentinel (len == SIZE_MAX) and does not claim content
 * with a NULL data pointer. Parsed catalogs never violate this; public
 * callers that build a catalog by hand can. */
static bool ident_span_ok(moq_bytes_t b)
{
    return b.len != SIZE_MAX && (b.len == 0 || b.data != NULL);
}

static bool track_identity_ok(const moq_msf_track_t *t)
{
    return ident_span_ok(t->name) &&
           (!t->has_namespace || ident_span_ok(t->namespace_));
}

/* Full Track Names unique within one array. MOQ_OK, MOQ_ERR_PROTO on the
 * first duplicate, MOQ_ERR_INVAL for an unusable span, MOQ_ERR_NOMEM. */
static moq_result_t names_unique(const moq_alloc_t *alloc,
                                 const msf_ident_mode_t *mode,
                                 const moq_msf_track_t *t, size_t n)
{
    if (n == 0) return MOQ_OK;
    for (size_t i = 0; i < n; i++)
        if (!track_identity_ok(&t[i])) return MOQ_ERR_INVAL;
    msf_index_t ix;
    moq_result_t rc = moq_msf_index_init(&ix, n, mode, alloc);
    if (rc < 0) return rc;
    for (size_t i = 0; i < n && rc == MOQ_OK; i++) {
        if (moq_msf_index_find(&ix, t, t[i].has_namespace, t[i].namespace_,
                               t[i].name) >= 0)
            rc = MOQ_ERR_PROTO;
        else
            moq_msf_index_insert(&ix, t, (long)i);
    }
    moq_msf_index_free(&ix, alloc);
    return rc;
}

/* Keys unique within one array of records (ids or refIDs). */
static moq_result_t spans_unique(const moq_alloc_t *alloc, size_t n,
                                 moq_bytes_t (*key_at)(const void *, size_t),
                                 const void *arr)
{
    if (n == 0) return MOQ_OK;
    for (size_t i = 0; i < n; i++)
        if (!ident_span_ok(key_at(arr, i))) return MOQ_ERR_INVAL;
    msf_span_index_t ix;
    moq_result_t rc = moq_msf_span_index_init(&ix, n, alloc);
    if (rc < 0) return rc;
    for (size_t i = 0; i < n && rc == MOQ_OK; i++)
        if (moq_msf_span_index_add(&ix, key_at(arr, i)) > 1) rc = MOQ_ERR_PROTO;
    moq_msf_span_index_free(&ix, alloc);
    return rc;
}

static moq_bytes_t init_id_at(const void *arr, size_t i)
{
    return ((const moq_msf_init_data_entry_t *)arr)[i].id;
}

static moq_bytes_t cp_ref_at(const void *arr, size_t i)
{
    return ((const moq_cmsf_content_protection_t *)arr)[i].ref_id;
}

/* The renderGroup values of `tracks`, as 4-byte keys of a span index. */
typedef struct { uint8_t b[sizeof(int)]; } group_key_t;

static moq_bytes_t group_bytes(const group_key_t *k)
{
    return (moq_bytes_t){ k->b, sizeof(k->b) };
}

static void group_key(group_key_t *k, int g)
{
    memcpy(k->b, &g, sizeof(g));
}

/* MSF-02 5.2.15: each String entry names exactly one OTHER track in `tracks`
 * (by name bytes, whatever its namespace); each Number entry is the
 * renderGroup of at least one track in `tracks`. */
static moq_result_t depends_resolve(const moq_alloc_t *alloc,
                                    const moq_msf_catalog_t *cat,
                                    moq_msf_catalog_error_t *reason)
{
    size_t n = cat->track_count;
    size_t groups = 0, entries = 0;
    for (size_t i = 0; i < n; i++) {
        const moq_msf_track_t *t = &cat->tracks[i];
        const moq_msf_track_supplement_t *s = moq_msf_supplement_of(t);
        if (t->depends_count > 0 && !t->depends) return MOQ_ERR_INVAL;
        if (s && s->depends_render_group_count > 0 && !s->depends_render_groups)
            return MOQ_ERR_INVAL;
        for (size_t k = 0; k < t->depends_count; k++)
            if (!ident_span_ok(t->depends[k])) return MOQ_ERR_INVAL;
        entries += t->depends_count + (s ? s->depends_render_group_count : 0);
        if (t->has_render_group) groups++;
    }
    if (entries == 0) return MOQ_OK;

    msf_span_index_t names, rg;
    moq_result_t rc = moq_msf_span_index_init(&names, n, alloc);
    if (rc < 0) return rc;
    rc = moq_msf_span_index_init(&rg, groups, alloc);
    if (rc < 0) { moq_msf_span_index_free(&names, alloc); return rc; }
    /* The group keys must outlive the index that borrows them. */
    group_key_t *gk = NULL;
    size_t gk_bytes = 0;
    if (groups > 0) {
        if (!moq_msf_checked_array_bytes(groups, sizeof(group_key_t), &gk_bytes) ||
            !(gk = (group_key_t *)alloc->alloc(gk_bytes, alloc->ctx)))
            rc = MOQ_ERR_NOMEM;
    }
    size_t g = 0;
    for (size_t i = 0; i < n && rc == MOQ_OK; i++) {
        const moq_msf_track_t *t = &cat->tracks[i];
        (void)moq_msf_span_index_add(&names, t->name);
        if (t->has_render_group) {
            group_key(&gk[g], t->render_group);
            (void)moq_msf_span_index_add(&rg, group_bytes(&gk[g]));
            g++;
        }
    }
    for (size_t i = 0; i < n && rc == MOQ_OK; i++) {
        const moq_msf_track_t *t = &cat->tracks[i];
        for (size_t k = 0; k < t->depends_count && rc == MOQ_OK; k++) {
            moq_bytes_t d = t->depends[k];
            size_t c = moq_msf_span_index_count(&names, d);
            /* The track itself is not a resolution of its own entry. */
            if (c > 0 && d.len == t->name.len &&
                (d.len == 0 || memcmp(d.data, t->name.data, d.len) == 0))
                c--;
            if (c == 0)
                rc = check_fail(reason, MOQ_MSF_CATALOG_ERR_DEPENDS_UNRESOLVED);
            else if (c > 1)
                rc = check_fail(reason, MOQ_MSF_CATALOG_ERR_DEPENDS_AMBIGUOUS);
        }
        const moq_msf_track_supplement_t *s = moq_msf_supplement_of(t);
        for (size_t k = 0; s && k < s->depends_render_group_count && rc == MOQ_OK;
             k++) {
            group_key_t key;
            group_key(&key, s->depends_render_groups[k]);
            if (moq_msf_span_index_count(&rg, group_bytes(&key)) == 0)
                rc = check_fail(reason, MOQ_MSF_CATALOG_ERR_DEPENDS_UNRESOLVED);
        }
    }
    if (gk) alloc->free(gk, gk_bytes, alloc->ctx);
    moq_msf_span_index_free(&rg, alloc);
    moq_msf_span_index_free(&names, alloc);
    return rc;
}

moq_result_t moq_msf_catalog_check(const moq_alloc_t *alloc,
                                   const moq_msf_catalog_t *cat,
                                   const moq_msf_validate_cfg_t *cfg,
                                   moq_msf_catalog_error_t *reason)
{
    if (reason) *reason = MOQ_MSF_CATALOG_OK;
    if (!alloc || !cat) return MOQ_ERR_INVAL;
    if (cat->delta_update) return MOQ_ERR_INVAL;   /* independent catalogs only */
    if (moq_msf_extent_of(cat->struct_size) == MSF_EXTENT_PARTIAL)
        return MOQ_ERR_INVAL;
    moq_msf_revision_t rev = moq_msf_catalog_revision_of(cat);
    if (rev != MOQ_MSF_REVISION_01 && rev != MOQ_MSF_REVISION_02)
        return MOQ_ERR_INVAL;
    moq_msf_validate_cfg_t c;
    moq_result_t rc = moq_msf_read_validate_cfg(cfg, &c);
    if (rc < 0) return rc;
    /* Public callers can build a catalog by hand: a nonzero count with a
     * NULL array is invalid input, refused before any dereference. */
    const moq_msf_track_t *pt;
    size_t pn = moq_msf_publish_tracks_of(cat, &pt);
    if ((cat->track_count > 0 && !cat->tracks) || (pn > 0 && !pt) ||
        (cat->init_data_count > 0 && !cat->init_data_list) ||
        (cat->content_protection_count > 0 && !cat->content_protections))
        return MOQ_ERR_INVAL;

    msf_ident_mode_t mode = { .fields = false, .catalog_ns = { NULL, 0 } };
    if (rev == MOQ_MSF_REVISION_02) {
        mode.fields = true;
        mode.catalog_ns = c.catalog_namespace;
    }
    /* 5.2.3 and 5.1.5: unique within tracks, and within publishTracks. */
    rc = names_unique(alloc, &mode, cat->tracks, cat->track_count);
    if (rc == MOQ_ERR_PROTO)
        return check_fail(reason, MOQ_MSF_CATALOG_ERR_DUPLICATE_TRACK);
    if (rc < 0) return rc;
    rc = names_unique(alloc, &mode, pt, pn);
    if (rc == MOQ_ERR_PROTO)
        return check_fail(reason, MOQ_MSF_CATALOG_ERR_DUPLICATE_PUBLISH_TRACK);
    if (rc < 0) return rc;
    /* 5.1.7: initDataList ids; CMSF 4.1.1.1: contentProtection refIDs. */
    rc = spans_unique(alloc, cat->init_data_count, init_id_at,
                      cat->init_data_list);
    if (rc == MOQ_ERR_PROTO)
        return check_fail(reason, MOQ_MSF_CATALOG_ERR_DUPLICATE_INIT_ID);
    if (rc < 0) return rc;
    rc = spans_unique(alloc, cat->content_protection_count, cp_ref_at,
                      cat->content_protections);
    if (rc == MOQ_ERR_PROTO)
        return check_fail(reason, MOQ_MSF_CATALOG_ERR_DUPLICATE_CP_REF);
    if (rc < 0) return rc;
    if (rev == MOQ_MSF_REVISION_02)
        return depends_resolve(alloc, cat, reason);
    return MOQ_OK;
}

moq_result_t moq_msf_catalog_validate_identities(const moq_alloc_t *alloc,
                                                 const moq_msf_catalog_t *cat)
{
    return moq_msf_catalog_check(alloc, cat, NULL, NULL);
}
