#define JSON_H_IMPLEMENTATION
#include "../vendor/json.h"

#include <moq/msf.h>
#include <moq/rcbuf.h>
#include <errno.h>
#include <float.h>
#include <limits.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* -- Overflow-safe array sizing -------------------------------------- */

/* Compute count*elem bytes for an array allocation, returning false (without
 * writing *out_bytes) when the product would wrap size_t. Every parser /
 * apply_delta array allocation is driven by an untrusted element count -- a
 * JSON array length, or an apply_delta capacity summed from per-op track
 * counts -- so each call site guards the size here and fails cleanly with
 * MOQ_ERR_NOMEM before a wrapped, undersized buffer could be allocated and
 * then written past. Not part of the public API (hidden visibility); exposed
 * unmangled only so the white-box unit test can exercise the boundary. */
bool moq_msf_checked_array_bytes(size_t count, size_t elem, size_t *out_bytes)
{
    if (elem != 0 && count > SIZE_MAX / elem)
        return false;
    *out_bytes = count * elem;
    return true;
}

/* -- Allocator bridge ------------------------------------------------ */

typedef struct {
    const moq_alloc_t *alloc;
    size_t             total;
} alloc_ctx_t;

static void *json_alloc_bridge(void *user, size_t size)
{
    alloc_ctx_t *ctx = (alloc_ctx_t *)user;
    ctx->total += size;
    return ctx->alloc->alloc(size, ctx->alloc->ctx);
}

/* -- Helpers --------------------------------------------------------- */

static moq_bytes_t bytes_from_json_string(const json_string_t *s)
{
    return (moq_bytes_t){
        .data = (const uint8_t *)s->string,
        .len = s->string_size,
    };
}

static bool str_eq(const json_string_t *s, const char *lit)
{
    size_t len = strlen(lit);
    return s->string_size == len && memcmp(s->string, lit, len) == 0;
}

static bool get_string(const json_object_element_t *e, moq_bytes_t *out)
{
    json_string_t *s = json_value_as_string(e->value);
    if (!s) return false;
    *out = bytes_from_json_string(s);
    return true;
}

static bool get_bool(const json_object_element_t *e, bool *out)
{
    if (json_value_is_true(e->value)) { *out = true; return true; }
    if (json_value_is_false(e->value)) { *out = false; return true; }
    return false;
}

static bool get_int(const json_object_element_t *e, int *out)
{
    json_number_t *n = json_value_as_number(e->value);
    if (!n || n->number_size == 0) return false;
    char *endp;
    errno = 0;
    long v = strtol(n->number, &endp, 10);
    if (errno == ERANGE) return false;
    if (endp != n->number + n->number_size) return false;
    if (v < INT_MIN || v > INT_MAX) return false;
    *out = (int)v;
    return true;
}

static bool get_u32(const json_object_element_t *e, uint32_t *out)
{
    json_number_t *n = json_value_as_number(e->value);
    if (!n || n->number_size == 0) return false;
    if (n->number[0] == '-') return false;
    char *endp;
    errno = 0;
    unsigned long v = strtoul(n->number, &endp, 10);
    if (errno == ERANGE) return false;
    if (endp != n->number + n->number_size) return false;
    if (v > UINT32_MAX) return false;
    *out = (uint32_t)v;
    return true;
}

static bool get_u64(const json_object_element_t *e, uint64_t *out)
{
    json_number_t *n = json_value_as_number(e->value);
    if (!n || n->number_size == 0) return false;
    if (n->number[0] == '-') return false;
    char *endp;
    errno = 0;
    unsigned long long v = strtoull(n->number, &endp, 10);
    if (errno == ERANGE) return false;
    if (endp != n->number + n->number_size) return false;
    *out = (uint64_t)v;
    return true;
}

static bool get_framerate_millis(const json_object_element_t *e,
                                 uint64_t *out)
{
    json_number_t *n = json_value_as_number(e->value);
    if (!n || n->number_size == 0) return false;
    if (n->number[0] == '-') return false;
    char *endp;
    errno = 0;
    double v = strtod(n->number, &endp);
    if (errno == ERANGE) return false;
    if (endp != n->number + n->number_size) return false;
    if (!isfinite(v) || v < 0.0 || v > 1000000.0) return false;
    *out = (uint64_t)(v * 1000.0 + 0.5);
    return true;
}

/*
 * MSF version handling (§5.1.1). "version" is the JSON String "1" -- the wire
 * form strict MSF-01 receivers (PlayA) accept and the form this encoder emits
 * (see the encoder). On parse this code also accepts "draft-01" (catalogs
 * emitted by intermediate libmoq builds that briefly used a draft-name string)
 * and the pre-01 numeric form (1) for LEGACY COMPATIBILITY ONLY (MSF-00
 * emitted a JSON number). Anything else -- other versions, other or malformed
 * draft strings, empty, or a non-string/non-number -- is unsupported; per
 * §5.1.1 a subscriber MUST NOT parse a version it does not understand.
 */
static bool version_is_supported(const json_object_element_t *e)
{
    json_string_t *s = json_value_as_string(e->value);
    if (s) return str_eq(s, "1") || str_eq(s, "draft-01");
    int v = 0;
    if (get_int(e, &v)) return v == MOQ_MSF_VERSION;   /* legacy MSF-00 numeric */
    return false;
}

/*
 * Materialize a JSON array of strings into an allocated moq_bytes_t array
 * (borrowing the string bytes from the DOM). Returns:
 *   MOQ_OK     - parsed (empty array -> NULL/0);
 *   MOQ_ERR_PROTO - value is not an array, or an element is not a string;
 *   MOQ_ERR_NOMEM - allocation failed.
 */
static moq_result_t get_string_array(const moq_alloc_t *alloc,
                                     const json_value_t *val,
                                     moq_bytes_t **out_arr, size_t *out_count)
{
    *out_arr = NULL;
    *out_count = 0;

    json_array_t *a = json_value_as_array((json_value_t *)val);
    if (!a) return MOQ_ERR_PROTO;
    if (a->length == 0) return MOQ_OK;

    size_t bytes;
    if (!moq_msf_checked_array_bytes(a->length, sizeof(moq_bytes_t), &bytes))
        return MOQ_ERR_NOMEM;
    moq_bytes_t *arr = (moq_bytes_t *)alloc->alloc(bytes, alloc->ctx);
    if (!arr) return MOQ_ERR_NOMEM;

    size_t n = 0;
    for (json_array_element_t *e = a->start; e; e = e->next) {
        json_string_t *s = json_value_as_string(e->value);
        if (!s) { alloc->free(arr, bytes, alloc->ctx); return MOQ_ERR_PROTO; }
        arr[n++] = bytes_from_json_string(s);
    }

    *out_arr = arr;
    *out_count = n;
    return MOQ_OK;
}

static void free_track_allocs(const moq_alloc_t *alloc, moq_msf_track_t *t)
{
    if (t->cp_ref_ids) {
        alloc->free(t->cp_ref_ids,
            t->cp_ref_id_count * sizeof(moq_bytes_t), alloc->ctx);
        t->cp_ref_ids = NULL;
        t->cp_ref_id_count = 0;
    }
    if (t->depends) {
        alloc->free(t->depends,
            t->depends_count * sizeof(moq_bytes_t), alloc->ctx);
        t->depends = NULL;
        t->depends_count = 0;
    }
}

static void free_cp_allocs(const moq_alloc_t *alloc,
                           moq_cmsf_content_protection_t *cp)
{
    if (cp->default_kids) {
        alloc->free(cp->default_kids,
            cp->default_kid_count * sizeof(moq_bytes_t), alloc->ctx);
        cp->default_kids = NULL;
        cp->default_kid_count = 0;
    }
}

/* CMSF laURL/certURL/authURL: an object with a required "url" string and an
 * optional "type" string. Called only when the key is present, so a non-object,
 * a missing/non-string url, or a non-string type is a protocol error. */
static moq_result_t parse_cmsf_url(const json_value_t *val, moq_cmsf_url_t *u)
{
    memset(u, 0, sizeof(*u));
    json_object_t *o = json_value_as_object((json_value_t *)val);
    if (!o) return MOQ_ERR_PROTO;
    bool got_url = false;
    for (json_object_element_t *m = o->start; m; m = m->next) {
        if (str_eq(m->name, "url")) {
            if (!get_string(m, &u->url)) return MOQ_ERR_PROTO;
            got_url = true;
        } else if (str_eq(m->name, "type")) {
            if (!get_string(m, &u->type)) return MOQ_ERR_PROTO;
            u->has_type = true;
        }
    }
    if (!got_url) return MOQ_ERR_PROTO;   /* url required when object present */
    u->present = true;
    return MOQ_OK;
}

/* CMSF drmSystem (§4.1.1.4); systemID is required. */
static moq_result_t parse_drm_system(const json_value_t *val,
                                     moq_cmsf_drm_system_t *ds)
{
    memset(ds, 0, sizeof(*ds));
    json_object_t *o = json_value_as_object((json_value_t *)val);
    if (!o) return MOQ_ERR_PROTO;

    bool got_sys = false;
    for (json_object_element_t *m = o->start; m; m = m->next) {
        if (str_eq(m->name, "systemID"))        got_sys = get_string(m, &ds->system_id);
        else if (str_eq(m->name, "laURL"))   { if (parse_cmsf_url(m->value, &ds->la_url)   < 0) return MOQ_ERR_PROTO; }
        else if (str_eq(m->name, "certURL")) { if (parse_cmsf_url(m->value, &ds->cert_url) < 0) return MOQ_ERR_PROTO; }
        else if (str_eq(m->name, "authURL")) { if (parse_cmsf_url(m->value, &ds->auth_url) < 0) return MOQ_ERR_PROTO; }
        else if (str_eq(m->name, "pssh"))       ds->has_pssh = get_string(m, &ds->pssh);
        else if (str_eq(m->name, "robustness")) ds->has_robustness = get_string(m, &ds->robustness);
    }

    if (!got_sys) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* CMSF content protection entry (§4.1.1); refID/defaultKID/scheme/drmSystem
 * are required (shape only -- values are not semantically validated). */
static moq_result_t parse_content_protection(const moq_alloc_t *alloc,
                                             const json_value_t *val,
                                             moq_cmsf_content_protection_t *cp)
{
    memset(cp, 0, sizeof(*cp));
    json_object_t *o = json_value_as_object((json_value_t *)val);
    if (!o) return MOQ_ERR_PROTO;

    bool got_ref = false, got_scheme = false, got_drm = false, got_kids = false;
    for (json_object_element_t *m = o->start; m; m = m->next) {
        if (str_eq(m->name, "refID")) {
            got_ref = get_string(m, &cp->ref_id);
        } else if (str_eq(m->name, "scheme")) {
            got_scheme = get_string(m, &cp->scheme);
        } else if (str_eq(m->name, "defaultKID")) {
            if (got_kids) {   /* duplicate key would leak the first array */
                free_cp_allocs(alloc, cp);
                return MOQ_ERR_PROTO;
            }
            moq_result_t rc = get_string_array(alloc, m->value,
                &cp->default_kids, &cp->default_kid_count);
            if (rc == MOQ_ERR_NOMEM) return rc;
            got_kids = (rc == MOQ_OK);
        } else if (str_eq(m->name, "drmSystem")) {
            got_drm = (parse_drm_system(m->value, &cp->drm_system) == MOQ_OK);
        }
    }

    if (!got_ref || !got_scheme || !got_drm || !got_kids) {
        free_cp_allocs(alloc, cp);
        return MOQ_ERR_PROTO;
    }
    return MOQ_OK;
}

/* -- Track parsing --------------------------------------------------- */

/* Track parse mode: an independent/add track requires name+packaging+isLive; a
 * clone op track requires name+parentName (packaging/isLive optional overrides);
 * a remove op track requires only name. */
typedef enum {
    PT_FULL = 0,  /* independent catalog track: name+packaging+isLive required */
    PT_ADD,       /* delta "add" track: only name required (packaging/isLive
                   * may be omitted -- the §5.6.4 example does) */
    PT_CLONE,     /* delta "clone" track: name + parentName required */
    PT_REMOVE,    /* delta "remove" track: only name (+optional namespace) */
} parse_track_mode_t;

/* True when a track carries nothing beyond name and (optionally) namespace --
 * the only fields a delta "remove" op track may hold (MSF 5.1.6). */
static bool track_is_name_namespace_only(const moq_msf_track_t *t)
{
    return !t->has_role && !t->has_codec && !t->has_init_data &&
           !t->has_init_track && !t->has_width && !t->has_height &&
           !t->has_samplerate && !t->has_bitrate && !t->has_framerate &&
           !t->has_render_group && !t->has_alt_group && !t->has_timescale &&
           !t->has_target_latency && !t->has_channel_config && !t->has_lang &&
           !t->has_label && !t->has_init_ref && !t->has_event_type &&
           !t->has_mime_type && t->depends_count == 0 &&
           t->cp_ref_id_count == 0 && !t->has_max_grp_sap &&
           !t->has_max_obj_sap && !t->has_parent_name &&
           !t->has_parent_namespace && !t->has_packaging && !t->has_is_live &&
           !t->has_template && !t->has_track_duration;
}

/* Parse a JSON number value as a non-negative integer uint64. Rejects a sign,
 * a fractional/exponent form (a non-numeric trailing char), and overflow. */
static bool num_value_u64(const json_value_t *v, uint64_t *out)
{
    json_number_t *n = json_value_as_number((json_value_t *)v);
    if (!n || n->number_size == 0) return false;
    if (n->number[0] == '-') return false;
    char *endp;
    errno = 0;
    unsigned long long x = strtoull(n->number, &endp, 10);
    if (errno == ERANGE) return false;
    if (endp != n->number + n->number_size) return false;
    *out = (uint64_t)x;
    return true;
}

/* Parse a MOQT Location pair [GroupID, ObjectID] (exactly two numbers). */
static bool parse_loc_pair(const json_value_t *v, uint64_t *g, uint64_t *o)
{
    json_array_t *a = json_value_as_array((json_value_t *)v);
    if (!a || a->length != 2) return false;
    json_array_element_t *e = a->start;
    if (!num_value_u64(e->value, g)) return false;
    e = e->next;
    if (!e || !num_value_u64(e->value, o)) return false;
    return true;
}

/* Parse the media timeline template (MSF 5.2.15 / §7.4): exactly six elements
 * [startMediaTime, deltaMediaTime, [g,o], [dg,do], startWallclock,
 * deltaWallclock]. A present-but-malformed template (wrong length, wrong nested
 * shape, non-integral value) is a PROTO error -- a structured field is never
 * treated as absent. */
static moq_result_t parse_template(const json_value_t *v,
                                   moq_msf_media_template_t *out)
{
    memset(out, 0, sizeof(*out));
    json_array_t *a = json_value_as_array((json_value_t *)v);
    if (!a || a->length != 6) return MOQ_ERR_PROTO;

    json_array_element_t *e = a->start;
    if (!num_value_u64(e->value, &out->start_media_ms)) return MOQ_ERR_PROTO;
    e = e->next;
    if (!e || !num_value_u64(e->value, &out->delta_media_ms)) return MOQ_ERR_PROTO;
    e = e->next;
    if (!e || !parse_loc_pair(e->value, &out->start_group, &out->start_object))
        return MOQ_ERR_PROTO;
    e = e->next;
    if (!e || !parse_loc_pair(e->value, &out->delta_group, &out->delta_object))
        return MOQ_ERR_PROTO;
    e = e->next;
    if (!e || !num_value_u64(e->value, &out->start_wallclock_ms))
        return MOQ_ERR_PROTO;
    e = e->next;
    if (!e || !num_value_u64(e->value, &out->delta_wallclock_ms))
        return MOQ_ERR_PROTO;
    return MOQ_OK;
}

static moq_result_t parse_track(const moq_alloc_t *alloc,
                                 const json_value_t *val,
                                 moq_msf_track_t *t,
                                 parse_track_mode_t mode)
{
    memset(t, 0, sizeof(*t));
    t->struct_size = sizeof(*t);

    json_object_t *obj = json_value_as_object((json_value_t *)val);
    if (!obj) return MOQ_ERR_PROTO;

    bool got_name = false, got_packaging = false, got_is_live = false;
    bool seen_cp_refs = false;
    bool seen_depends = false;
    bool seen_template = false;

    for (json_object_element_t *e = obj->start; e; e = e->next) {
        if (str_eq(e->name, "name")) {
            got_name = get_string(e, &t->name);
        } else if (str_eq(e->name, "packaging")) {
            got_packaging = get_string(e, &t->packaging);
        } else if (str_eq(e->name, "isLive")) {
            got_is_live = get_bool(e, &t->is_live);
        } else if (str_eq(e->name, "namespace")) {
            t->has_namespace = get_string(e, &t->namespace_);
        } else if (str_eq(e->name, "role")) {
            t->has_role = get_string(e, &t->role);
        } else if (str_eq(e->name, "codec")) {
            t->has_codec = get_string(e, &t->codec);
        } else if (str_eq(e->name, "initData")) {
            t->has_init_data = get_string(e, &t->init_data);
        } else if (str_eq(e->name, "initTrack")) {
            t->has_init_track = get_string(e, &t->init_track);
        } else if (str_eq(e->name, "width")) {
            t->has_width = get_u32(e, &t->width);
        } else if (str_eq(e->name, "height")) {
            t->has_height = get_u32(e, &t->height);
        } else if (str_eq(e->name, "samplerate")) {
            t->has_samplerate = get_u32(e, &t->samplerate);
        } else if (str_eq(e->name, "bitrate")) {
            t->has_bitrate = get_u64(e, &t->bitrate);
        } else if (str_eq(e->name, "framerate")) {
            t->has_framerate = get_framerate_millis(e, &t->framerate_millis);
        } else if (str_eq(e->name, "renderGroup")) {
            t->has_render_group = get_int(e, &t->render_group);
        } else if (str_eq(e->name, "altGroup")) {
            t->has_alt_group = get_int(e, &t->alt_group);
        } else if (str_eq(e->name, "timescale")) {
            t->has_timescale = get_u64(e, &t->timescale);
        } else if (str_eq(e->name, "targetLatency")) {
            t->has_target_latency = get_u64(e, &t->target_latency);
        } else if (str_eq(e->name, "trackDuration")) {
            t->has_track_duration = get_u64(e, &t->track_duration_ms);
        } else if (str_eq(e->name, "channelConfig")) {
            t->has_channel_config = get_string(e, &t->channel_config);
        } else if (str_eq(e->name, "lang")) {
            t->has_lang = get_string(e, &t->lang);
        } else if (str_eq(e->name, "label")) {
            t->has_label = get_string(e, &t->label);
        } else if (str_eq(e->name, "initRef")) {
            t->has_init_ref = get_string(e, &t->init_ref);
        } else if (str_eq(e->name, "eventType")) {
            t->has_event_type = get_string(e, &t->event_type);
        } else if (str_eq(e->name, "mimeType") || str_eq(e->name, "mimetype")) {
            /* Canonical key is "mimeType"; MSF examples also use "mimetype",
             * so accept both leniently. */
            t->has_mime_type = get_string(e, &t->mime_type);
        } else if (str_eq(e->name, "depends")) {
            if (seen_depends) return MOQ_ERR_PROTO;  /* duplicate; caller frees first */
            seen_depends = true;
            moq_result_t rc = get_string_array(alloc, e->value,
                &t->depends, &t->depends_count);
            if (rc < 0) return rc;   /* present but not an array of strings */
        } else if (str_eq(e->name, "contentProtectionRefIDs")) {
            if (seen_cp_refs) return MOQ_ERR_PROTO;  /* duplicate; caller frees first */
            seen_cp_refs = true;
            moq_result_t rc = get_string_array(alloc, e->value,
                &t->cp_ref_ids, &t->cp_ref_id_count);
            if (rc < 0) return rc;   /* present but not an array of strings */
        } else if (str_eq(e->name, "maxGrpSapStartingType")) {
            t->has_max_grp_sap = get_u32(e, &t->max_grp_sap);
        } else if (str_eq(e->name, "maxObjSapStartingType")) {
            t->has_max_obj_sap = get_u32(e, &t->max_obj_sap);
        } else if (str_eq(e->name, "template")) {
            /* MSF 5.2.15 / §7.4: a present template MUST be a well-formed
             * 6-tuple; malformed or duplicate is a PROTO error (not absent). */
            if (seen_template) return MOQ_ERR_PROTO;
            seen_template = true;
            moq_result_t rc = parse_template(e->value, &t->template_);
            if (rc < 0) return rc;
            t->has_template = true;
        } else if (str_eq(e->name, "parentName")) {
            t->has_parent_name = get_string(e, &t->parent_name);
        } else if (str_eq(e->name, "parentNamespace")) {
            t->has_parent_namespace = get_string(e, &t->parent_namespace);
        }
    }

    /* Record presence of the otherwise-required keys for clone-override
     * fidelity (the encoder still emits them unconditionally for full/add). */
    t->has_packaging = got_packaging;
    t->has_is_live = got_is_live;

    /* MSF 5.2.33/34: parentName/parentNamespace MUST only appear in a clone op. */
    if (mode != PT_CLONE && (t->has_parent_name || t->has_parent_namespace))
        return MOQ_ERR_PROTO;

    switch (mode) {
    case PT_FULL:
        if (!got_name || !got_packaging || !got_is_live) return MOQ_ERR_PROTO;
        break;
    case PT_ADD:
        /* MSF 5.1.6 add: a track object, but the §5.6.4 example omits the
         * (normally required) packaging key, so only name is enforced here.
         * A packaging-less add yields a track the encoder still rejects, so
         * such a delta parses/round-trips but cannot be applied. */
        if (!got_name) return MOQ_ERR_PROTO;
        break;
    case PT_CLONE:
        /* MSF 5.1.6 clone: parentName is required and the new name is
         * required; packaging/isLive are inherited unless overridden. */
        if (!got_name || !t->has_parent_name) return MOQ_ERR_PROTO;
        break;
    case PT_REMOVE:
        /* MSF 5.1.6 remove: MUST include name, MAY include namespace, MUST NOT
         * hold any other field. */
        if (!got_name || !track_is_name_namespace_only(t)) return MOQ_ERR_PROTO;
        break;
    }

    return MOQ_OK;
}

/* Parse one Initialization Data List entry (MSF 5.1.7): {id, type, data}. */
static moq_result_t parse_init_data_entry(const json_value_t *val,
                                          moq_msf_init_data_entry_t *e)
{
    memset(e, 0, sizeof(*e));

    json_object_t *obj = json_value_as_object((json_value_t *)val);
    if (!obj) return MOQ_ERR_PROTO;

    bool got_id = false, got_type = false, got_data = false;
    for (json_object_element_t *m = obj->start; m; m = m->next) {
        if (str_eq(m->name, "id"))        got_id   = get_string(m, &e->id);
        else if (str_eq(m->name, "type")) got_type = get_string(m, &e->type);
        else if (str_eq(m->name, "data")) got_data = get_string(m, &e->data);
    }

    if (!got_id || !got_type || !got_data)
        return MOQ_ERR_PROTO;
    return MOQ_OK;
}

static bool bytes_eq_lit(moq_bytes_t v, const char *lit);  /* defined below */

/* Free a delta op's per-track allocations and its track array. */
static void free_delta_op_allocs(const moq_alloc_t *alloc,
                                 moq_msf_delta_op_t *op)
{
    if (!op->tracks) return;
    for (size_t i = 0; i < op->track_count; i++)
        free_track_allocs(alloc, &op->tracks[i]);
    alloc->free(op->tracks, op->track_count * sizeof(moq_msf_track_t),
                alloc->ctx);
    op->tracks = NULL;
    op->track_count = 0;
}

/* Parse one deltaUpdate operation object (MSF 5.1.6): {op, tracks:[...]}. */
static moq_result_t parse_delta_op(const moq_alloc_t *alloc,
                                   const json_value_t *val,
                                   moq_msf_delta_op_t *out)
{
    memset(out, 0, sizeof(*out));

    json_object_t *obj = json_value_as_object((json_value_t *)val);
    if (!obj) return MOQ_ERR_PROTO;

    moq_bytes_t op_str = { NULL, 0 };
    bool got_op = false;
    json_array_t *tracks_arr = NULL;
    bool seen_tracks = false;

    for (json_object_element_t *e = obj->start; e; e = e->next) {
        if (str_eq(e->name, "op")) {
            got_op = get_string(e, &op_str);
        } else if (str_eq(e->name, "tracks")) {
            if (seen_tracks) return MOQ_ERR_PROTO;  /* duplicate */
            seen_tracks = true;
            tracks_arr = json_value_as_array(e->value);
            if (!tracks_arr) return MOQ_ERR_PROTO;   /* present but not array */
        }
    }

    if (!got_op || !tracks_arr) return MOQ_ERR_PROTO;

    parse_track_mode_t mode;
    if (bytes_eq_lit(op_str, "add"))         { out->op = MOQ_MSF_DELTA_OP_ADD;    mode = PT_ADD; }
    else if (bytes_eq_lit(op_str, "remove")) { out->op = MOQ_MSF_DELTA_OP_REMOVE; mode = PT_REMOVE; }
    else if (bytes_eq_lit(op_str, "clone"))  { out->op = MOQ_MSF_DELTA_OP_CLONE;  mode = PT_CLONE; }
    else return MOQ_ERR_PROTO;                /* unknown op type */

    size_t n = tracks_arr->length;
    if (n == 0) return MOQ_ERR_PROTO;         /* an op MUST carry >= 1 track */

    size_t tbytes;
    if (!moq_msf_checked_array_bytes(n, sizeof(moq_msf_track_t), &tbytes))
        return MOQ_ERR_NOMEM;
    moq_msf_track_t *tracks = (moq_msf_track_t *)alloc->alloc(
        tbytes, alloc->ctx);
    if (!tracks) return MOQ_ERR_NOMEM;

    size_t parsed = 0;
    json_array_element_t *ae = tracks_arr->start;
    for (size_t i = 0; i < n && ae; i++, ae = ae->next) {
        moq_result_t rc = parse_track(alloc, ae->value, &tracks[parsed], mode);
        if (rc < 0) {
            free_track_allocs(alloc, &tracks[parsed]);
            for (size_t j = 0; j < parsed; j++) free_track_allocs(alloc, &tracks[j]);
            alloc->free(tracks, tbytes, alloc->ctx);
            return rc;
        }
        parsed++;
    }

    out->tracks = tracks;
    out->track_count = parsed;
    return MOQ_OK;
}

/* -- Public API ------------------------------------------------------ */

moq_result_t moq_msf_catalog_parse(const moq_alloc_t *alloc,
                                    moq_bytes_t json,
                                    moq_msf_catalog_t *out)
{
    if (!alloc || !json.data || !out) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    out->struct_size = sizeof(*out);

    alloc_ctx_t actx = { .alloc = alloc, .total = 0 };
    json_parse_result_t err;
    json_value_t *root = json_parse_ex(
        json.data, json.len, json_parse_flags_default,
        json_alloc_bridge, &actx, &err);
    if (!root) {
        if (err.error == json_parse_error_allocator_failed)
            return MOQ_ERR_NOMEM;
        return MOQ_ERR_PROTO;
    }
    out->_dom = root;
    out->_dom_size = actx.total;

    json_object_t *top = json_value_as_object(root);
    if (!top) {
        moq_msf_catalog_cleanup(alloc, out);
        return MOQ_ERR_PROTO;
    }

    bool got_version = false;
    bool seen_tracks = false;
    bool seen_is_complete = false;
    json_array_t *tracks_arr = NULL;
    json_array_t *idl_arr = NULL;
    json_array_t *cp_arr = NULL;
    json_array_t *delta_arr = NULL;

    for (json_object_element_t *e = top->start; e; e = e->next) {
        if (str_eq(e->name, "version")) {
            if (!version_is_supported(e)) {
                moq_msf_catalog_cleanup(alloc, out);
                return MOQ_ERR_PROTO;
            }
            out->version = MOQ_MSF_VERSION;
            got_version = true;
        } else if (str_eq(e->name, "tracks")) {
            seen_tracks = true;
            tracks_arr = json_value_as_array(e->value);
        } else if (str_eq(e->name, "generatedAt")) {
            out->has_generated_at = get_u64(e, &out->generated_at);
        } else if (str_eq(e->name, "isComplete")) {
            /* MSF 5.1.3: only the TRUE state is meaningful; a false/malformed
             * value is treated as absent (the spec forbids emitting false). The
             * key's presence is still recorded so a delta carrying it is
             * rejected below. */
            seen_is_complete = true;
            bool b = false;
            if (get_bool(e, &b) && b) out->is_complete = true;
        } else if (str_eq(e->name, "initDataList")) {
            idl_arr = json_value_as_array(e->value);
            if (!idl_arr) {   /* present but not an array */
                moq_msf_catalog_cleanup(alloc, out);
                return MOQ_ERR_PROTO;
            }
        } else if (str_eq(e->name, "contentProtections")) {
            cp_arr = json_value_as_array(e->value);
            if (!cp_arr) {    /* present but not an array */
                moq_msf_catalog_cleanup(alloc, out);
                return MOQ_ERR_PROTO;
            }
        } else if (str_eq(e->name, "deltaUpdate")) {
            delta_arr = json_value_as_array(e->value);
            if (!delta_arr) {  /* present but not an array */
                moq_msf_catalog_cleanup(alloc, out);
                return MOQ_ERR_PROTO;
            }
        }
    }

    /* MSF 5.1.6/5.3: deltaUpdate marks a partial update. A delta MUST NOT carry
     * a version, tracks, or isComplete field and MUST hold at least one
     * operation (5.1.3: completion is signalled by an independent catalog). */
    if (delta_arr) {
        if (got_version || seen_tracks || seen_is_complete ||
            delta_arr->length == 0) {
            moq_msf_catalog_cleanup(alloc, out);
            return MOQ_ERR_PROTO;
        }
        size_t oc = delta_arr->length;
        size_t ob;
        if (!moq_msf_checked_array_bytes(oc, sizeof(moq_msf_delta_op_t), &ob)) {
            moq_msf_catalog_cleanup(alloc, out);
            return MOQ_ERR_NOMEM;
        }
        moq_msf_delta_op_t *ops = (moq_msf_delta_op_t *)alloc->alloc(
            ob, alloc->ctx);
        if (!ops) {
            moq_msf_catalog_cleanup(alloc, out);
            return MOQ_ERR_NOMEM;
        }
        size_t poc = 0;
        json_array_element_t *oe = delta_arr->start;
        for (size_t i = 0; i < oc && oe; i++, oe = oe->next) {
            moq_result_t rc = parse_delta_op(alloc, oe->value, &ops[poc]);
            if (rc < 0) {
                for (size_t j = 0; j < poc; j++) free_delta_op_allocs(alloc, &ops[j]);
                alloc->free(ops, ob, alloc->ctx);
                moq_msf_catalog_cleanup(alloc, out);
                return rc;
            }
            poc++;
        }
        out->delta_update = ops;
        out->delta_update_count = poc;
        return MOQ_OK;   /* a delta carries no tracks/idl/cp of its own */
    }

    if (!got_version || !tracks_arr) {
        moq_msf_catalog_cleanup(alloc, out);
        return MOQ_ERR_PROTO;
    }

    size_t count = tracks_arr->length;
    if (count == 0) {
        out->tracks = NULL;
        out->track_count = 0;
    } else {
        size_t tb;
        if (!moq_msf_checked_array_bytes(count, sizeof(moq_msf_track_t), &tb)) {
            moq_msf_catalog_cleanup(alloc, out);
            return MOQ_ERR_NOMEM;
        }
        moq_msf_track_t *tracks = (moq_msf_track_t *)alloc->alloc(
            tb, alloc->ctx);
        if (!tracks) {
            moq_msf_catalog_cleanup(alloc, out);
            return MOQ_ERR_NOMEM;
        }

        size_t parsed = 0;
        json_array_element_t *ae = tracks_arr->start;
        for (size_t i = 0; i < count && ae; i++, ae = ae->next) {
            moq_result_t rc = parse_track(alloc, ae->value, &tracks[parsed],
                                          PT_FULL);
            if (rc < 0) {
                free_track_allocs(alloc, &tracks[parsed]); /* the failed track */
                for (size_t j = 0; j < parsed; j++)
                    free_track_allocs(alloc, &tracks[j]);
                alloc->free(tracks, tb, alloc->ctx);
                moq_msf_catalog_cleanup(alloc, out);
                return rc;
            }
            parsed++;
        }

        out->tracks = tracks;
        out->track_count = parsed;
    }

    /* Initialization Data List (MSF 5.1.7) -- present after tracks in the
     * document; parse order here is independent of that. */
    if (idl_arr && idl_arr->length > 0) {
        size_t ic = idl_arr->length;
        size_t ib;
        if (!moq_msf_checked_array_bytes(ic, sizeof(moq_msf_init_data_entry_t),
                                         &ib)) {
            moq_msf_catalog_cleanup(alloc, out);
            return MOQ_ERR_NOMEM;
        }
        moq_msf_init_data_entry_t *entries =
            (moq_msf_init_data_entry_t *)alloc->alloc(ib, alloc->ctx);
        if (!entries) {
            moq_msf_catalog_cleanup(alloc, out);
            return MOQ_ERR_NOMEM;
        }

        size_t pic = 0;
        json_array_element_t *ie = idl_arr->start;
        for (size_t i = 0; i < ic && ie; i++, ie = ie->next) {
            moq_result_t rc = parse_init_data_entry(ie->value, &entries[pic]);
            if (rc < 0) {
                alloc->free(entries, ib, alloc->ctx);
                moq_msf_catalog_cleanup(alloc, out);
                return rc;
            }
            pic++;
        }

        out->init_data_list = entries;
        out->init_data_count = pic;
    }

    /* Content protections (CMSF §4.1.1). */
    if (cp_arr && cp_arr->length > 0) {
        size_t cc = cp_arr->length;
        size_t cb;
        if (!moq_msf_checked_array_bytes(
                cc, sizeof(moq_cmsf_content_protection_t), &cb)) {
            moq_msf_catalog_cleanup(alloc, out);
            return MOQ_ERR_NOMEM;
        }
        moq_cmsf_content_protection_t *cps =
            (moq_cmsf_content_protection_t *)alloc->alloc(cb, alloc->ctx);
        if (!cps) {
            moq_msf_catalog_cleanup(alloc, out);
            return MOQ_ERR_NOMEM;
        }

        size_t pcc = 0;
        json_array_element_t *ce = cp_arr->start;
        for (size_t i = 0; i < cc && ce; i++, ce = ce->next) {
            moq_result_t rc = parse_content_protection(alloc, ce->value, &cps[pcc]);
            if (rc < 0) {
                for (size_t j = 0; j < pcc; j++) free_cp_allocs(alloc, &cps[j]);
                alloc->free(cps, cb, alloc->ctx);
                moq_msf_catalog_cleanup(alloc, out);
                return rc;
            }
            pcc++;
        }

        out->content_protections = cps;
        out->content_protection_count = pcc;
    }

    return MOQ_OK;
}

void moq_msf_catalog_cleanup(const moq_alloc_t *alloc,
                              moq_msf_catalog_t *cat)
{
    if (!alloc || !cat) return;
    if (cat->tracks) {
        for (size_t i = 0; i < cat->track_count; i++)
            free_track_allocs(alloc, &cat->tracks[i]);
        alloc->free(cat->tracks,
            cat->track_count * sizeof(moq_msf_track_t), alloc->ctx);
        cat->tracks = NULL;
    }
    if (cat->content_protections) {
        for (size_t i = 0; i < cat->content_protection_count; i++)
            free_cp_allocs(alloc, &cat->content_protections[i]);
        alloc->free(cat->content_protections,
            cat->content_protection_count *
                sizeof(moq_cmsf_content_protection_t), alloc->ctx);
        cat->content_protections = NULL;
    }
    cat->content_protection_count = 0;
    if (cat->delta_update) {
        for (size_t i = 0; i < cat->delta_update_count; i++)
            free_delta_op_allocs(alloc, &cat->delta_update[i]);
        alloc->free(cat->delta_update,
            cat->delta_update_count * sizeof(moq_msf_delta_op_t), alloc->ctx);
        cat->delta_update = NULL;
    }
    cat->delta_update_count = 0;
    if (cat->init_data_list) {
        alloc->free(cat->init_data_list,
            cat->init_data_count * sizeof(moq_msf_init_data_entry_t),
            alloc->ctx);
        cat->init_data_list = NULL;
    }
    cat->init_data_count = 0;
    if (cat->_dom) {
        alloc->free(cat->_dom, cat->_dom_size, alloc->ctx);
        cat->_dom = NULL;
        cat->_dom_size = 0;
    }
    cat->track_count = 0;
}

/* Match a track in list[0..n) by name, additionally requiring namespace when
 * has_ns (catalog-level namespace inheritance is not resolved). Returns the
 * index, or -1 if absent. */
static long find_track_idx(const moq_msf_track_t *list, size_t n,
                           moq_bytes_t name, bool has_ns, moq_bytes_t ns)
{
    for (size_t i = 0; i < n; i++) {
        const moq_msf_track_t *t = &list[i];
        if (t->name.len != name.len ||
            (name.len && memcmp(t->name.data, name.data, name.len) != 0))
            continue;
        if (has_ns) {
            if (!t->has_namespace) continue;
            if (t->namespace_.len != ns.len ||
                (ns.len && memcmp(t->namespace_.data, ns.data, ns.len) != 0))
                continue;
        }
        return (long)i;
    }
    return -1;
}

/* Build an effective track from a clone parent plus the clone op's overrides.
 * eff begins as a copy of the parent; spans are borrowed (no allocation). */
static void apply_clone_overrides(moq_msf_track_t *eff,
                                  const moq_msf_track_t *c)
{
    eff->name = c->name;                 /* the new track name is mandatory */
    /* Parent markers do not belong on an effective independent track. */
    eff->has_parent_name = false;
    eff->has_parent_namespace = false;
    eff->parent_name = (moq_bytes_t){ NULL, 0 };
    eff->parent_namespace = (moq_bytes_t){ NULL, 0 };

    if (c->has_packaging) eff->packaging = c->packaging;
    if (c->has_is_live)   eff->is_live = c->is_live;
    eff->has_packaging = true;           /* an effective full track has both */
    eff->has_is_live = true;

    if (c->has_namespace)      { eff->has_namespace = true; eff->namespace_ = c->namespace_; }
    if (c->has_role)           { eff->has_role = true; eff->role = c->role; }
    if (c->has_codec)          { eff->has_codec = true; eff->codec = c->codec; }
    if (c->has_init_data)      { eff->has_init_data = true; eff->init_data = c->init_data; }
    if (c->has_init_track)     { eff->has_init_track = true; eff->init_track = c->init_track; }
    if (c->has_width)          { eff->has_width = true; eff->width = c->width; }
    if (c->has_height)         { eff->has_height = true; eff->height = c->height; }
    if (c->has_samplerate)     { eff->has_samplerate = true; eff->samplerate = c->samplerate; }
    if (c->has_bitrate)        { eff->has_bitrate = true; eff->bitrate = c->bitrate; }
    if (c->has_framerate)      { eff->has_framerate = true; eff->framerate_millis = c->framerate_millis; }
    if (c->has_render_group)   { eff->has_render_group = true; eff->render_group = c->render_group; }
    if (c->has_alt_group)      { eff->has_alt_group = true; eff->alt_group = c->alt_group; }
    if (c->has_timescale)      { eff->has_timescale = true; eff->timescale = c->timescale; }
    if (c->has_target_latency) { eff->has_target_latency = true; eff->target_latency = c->target_latency; }
    if (c->has_track_duration) { eff->has_track_duration = true; eff->track_duration_ms = c->track_duration_ms; }
    if (c->has_channel_config) { eff->has_channel_config = true; eff->channel_config = c->channel_config; }
    if (c->has_lang)           { eff->has_lang = true; eff->lang = c->lang; }
    if (c->has_label)          { eff->has_label = true; eff->label = c->label; }
    if (c->has_init_ref)       { eff->has_init_ref = true; eff->init_ref = c->init_ref; }
    if (c->has_event_type)     { eff->has_event_type = true; eff->event_type = c->event_type; }
    if (c->has_mime_type)      { eff->has_mime_type = true; eff->mime_type = c->mime_type; }
    if (c->has_max_grp_sap)    { eff->has_max_grp_sap = true; eff->max_grp_sap = c->max_grp_sap; }
    if (c->has_max_obj_sap)    { eff->has_max_obj_sap = true; eff->max_obj_sap = c->max_obj_sap; }
    if (c->depends_count > 0)  { eff->depends = c->depends; eff->depends_count = c->depends_count; }
    if (c->cp_ref_id_count > 0){ eff->cp_ref_ids = c->cp_ref_ids; eff->cp_ref_id_count = c->cp_ref_id_count; }
    /* eff began as a copy of the parent, so it already inherits the parent's
     * template; a clone carrying its own template overrides it (5.2.15). */
    if (c->has_template)       { eff->has_template = true; eff->template_ = c->template_; }
}

/* Internal encode: strict_cp=true enforces CMSF §4 contentProtection authoring
 * syntax; false keeps it lenient for the receive-path re-encode below. */
static moq_result_t catalog_encode_impl(const moq_alloc_t *alloc,
                                        const moq_msf_catalog_t *cat,
                                        moq_rcbuf_t **out_json, bool strict_cp);

moq_result_t moq_msf_catalog_apply_delta(const moq_alloc_t *alloc,
                                          const moq_msf_catalog_t *base,
                                          const moq_msf_catalog_t *delta,
                                          moq_msf_catalog_t *out)
{
    if (!alloc || !base || !delta || !out) return MOQ_ERR_INVAL;
    if (base->delta_update) return MOQ_ERR_INVAL;     /* base must be independent */
    if (!delta->delta_update) return MOQ_ERR_INVAL;   /* delta must be a delta */
    /* MSF 5.1.3: isComplete is terminal -- a complete catalog commits that no
     * tracks or content will follow, so applying any delta to it is a protocol
     * error (and would otherwise drop the MUST-NOT-be-removed isComplete). */
    if (base->is_complete) return MOQ_ERR_PROTO;
    memset(out, 0, sizeof(*out));

    /* Capacity = base tracks + every add/clone track (removes only shrink).
     * Guard the running sum against size_t wrap, then the byte size against
     * wrap, so a crafted base/delta cannot under-allocate eff and overflow it
     * during the copy/apply loop below. */
    size_t cap = base->track_count;
    for (size_t i = 0; i < delta->delta_update_count; i++) {
        const moq_msf_delta_op_t *op = &delta->delta_update[i];
        if (op->op == MOQ_MSF_DELTA_OP_ADD || op->op == MOQ_MSF_DELTA_OP_CLONE) {
            if (op->track_count > SIZE_MAX - cap)
                return MOQ_ERR_NOMEM;
            cap += op->track_count;
        }
    }

    moq_msf_track_t *eff = NULL;
    size_t eff_bytes = 0;
    if (cap > 0) {
        if (!moq_msf_checked_array_bytes(cap, sizeof(moq_msf_track_t),
                                         &eff_bytes))
            return MOQ_ERR_NOMEM;
        eff = (moq_msf_track_t *)alloc->alloc(eff_bytes, alloc->ctx);
        if (!eff) return MOQ_ERR_NOMEM;
    }
    size_t n = 0;
    for (size_t i = 0; i < base->track_count; i++) eff[n++] = base->tracks[i];

    moq_result_t rc = MOQ_OK;
    for (size_t i = 0; i < delta->delta_update_count && rc == MOQ_OK; i++) {
        const moq_msf_delta_op_t *op = &delta->delta_update[i];
        for (size_t j = 0; j < op->track_count; j++) {
            const moq_msf_track_t *ot = &op->tracks[j];
            if (op->op == MOQ_MSF_DELTA_OP_ADD) {
                /* 5.1.6/5.3: add declares a NEW tuple -- a collision is a bad
                 * delta (the tuple is immutable once declared). */
                if (find_track_idx(eff, n, ot->name,
                        ot->has_namespace, ot->namespace_) >= 0) {
                    rc = MOQ_ERR_PROTO; break;
                }
                eff[n++] = *ot;
            } else if (op->op == MOQ_MSF_DELTA_OP_REMOVE) {
                long idx = find_track_idx(eff, n, ot->name,
                                          ot->has_namespace, ot->namespace_);
                if (idx < 0) { rc = MOQ_ERR_PROTO; break; }  /* not declared */
                for (size_t k = (size_t)idx; k + 1 < n; k++) eff[k] = eff[k + 1];
                n--;
            } else { /* CLONE */
                long idx = find_track_idx(eff, n, ot->parent_name,
                    ot->has_parent_namespace, ot->parent_namespace);
                if (idx < 0) { rc = MOQ_ERR_PROTO; break; }  /* parent absent */
                moq_msf_track_t e = eff[(size_t)idx];
                apply_clone_overrides(&e, ot);
                /* The cloned tuple MUST be new. */
                if (find_track_idx(eff, n, e.name,
                        e.has_namespace, e.namespace_) >= 0) {
                    rc = MOQ_ERR_PROTO; break;
                }
                eff[n++] = e;
            }
        }
    }

    moq_rcbuf_t *json = NULL;
    if (rc == MOQ_OK) {
        moq_msf_catalog_t mid;
        memset(&mid, 0, sizeof(mid));
        mid.struct_size = sizeof(mid);
        mid.version = MOQ_MSF_VERSION;
        mid.tracks = eff;
        mid.track_count = n;
        if (delta->has_generated_at) {
            mid.has_generated_at = true; mid.generated_at = delta->generated_at;
        } else if (base->has_generated_at) {
            mid.has_generated_at = true; mid.generated_at = base->generated_at;
        }
        mid.init_data_list = base->init_data_list;
        mid.init_data_count = base->init_data_count;
        mid.content_protections = base->content_protections;
        mid.content_protection_count = base->content_protection_count;
        /* Receive path: re-encode the (leniently-parsed) base CP as-is so an
         * inbound catalog with non-CMSF-strict contentProtection still applies
         * its delta. Authoring strictness is enforced only via the public
         * moq_msf_catalog_encode. */
        rc = catalog_encode_impl(alloc, &mid, &json, /*strict_cp=*/false);
    }

    if (eff) alloc->free(eff, eff_bytes, alloc->ctx);
    if (rc < 0) return rc;

    moq_bytes_t jb = { moq_rcbuf_data(json), moq_rcbuf_len(json) };
    rc = moq_msf_catalog_parse(alloc, jb, out);
    moq_rcbuf_decref(json);
    return rc;
}

const moq_msf_track_t *moq_msf_catalog_find_role(
    const moq_msf_catalog_t *cat, const char *role)
{
    if (!cat || !role) return NULL;
    size_t rlen = strlen(role);
    for (size_t i = 0; i < cat->track_count; i++) {
        const moq_msf_track_t *t = &cat->tracks[i];
        if (t->has_role && t->role.len == rlen &&
            memcmp(t->role.data, role, rlen) == 0)
            return t;
    }
    return NULL;
}

const moq_msf_init_data_entry_t *moq_msf_catalog_find_init_data(
    const moq_msf_catalog_t *cat, moq_bytes_t id)
{
    if (!cat || !id.data || id.len == 0) return NULL;
    if (cat->init_data_count > 0 && !cat->init_data_list) return NULL;
    for (size_t i = 0; i < cat->init_data_count; i++) {
        const moq_msf_init_data_entry_t *e = &cat->init_data_list[i];
        if (e->id.len == id.len && e->id.data &&
            memcmp(e->id.data, id.data, id.len) == 0)
            return e;
    }
    return NULL;
}

const moq_cmsf_content_protection_t *moq_msf_catalog_find_content_protection(
    const moq_msf_catalog_t *cat, moq_bytes_t ref_id)
{
    if (!cat || !ref_id.data || ref_id.len == 0) return NULL;
    if (cat->content_protection_count > 0 && !cat->content_protections) return NULL;
    for (size_t i = 0; i < cat->content_protection_count; i++) {
        const moq_cmsf_content_protection_t *cp = &cat->content_protections[i];
        if (cp->ref_id.len == ref_id.len && cp->ref_id.data &&
            memcmp(cp->ref_id.data, ref_id.data, ref_id.len) == 0)
            return cp;
    }
    return NULL;
}

/* -- Encoder --------------------------------------------------------- */

typedef struct {
    uint8_t *buf;
    size_t   cap;
    size_t   pos;
    bool     overflow;
} enc_buf_t;

static void enc_raw(enc_buf_t *b, const char *s, size_t len)
{
    if (len > SIZE_MAX - b->pos) { b->overflow = true; return; }
    if (b->buf) {
        if (b->pos + len > b->cap) { b->overflow = true; return; }
        memcpy(b->buf + b->pos, s, len);
    }
    b->pos += len;
}

static void enc_lit(enc_buf_t *b, const char *s)
{
    enc_raw(b, s, strlen(s));
}

static void enc_char(enc_buf_t *b, char c)
{
    if (b->pos == SIZE_MAX) { b->overflow = true; return; }
    if (b->buf) {
        if (b->pos >= b->cap) { b->overflow = true; return; }
        b->buf[b->pos] = (uint8_t)c;
    }
    b->pos++;
}

static void enc_json_string(enc_buf_t *b, const uint8_t *data, size_t len)
{
    enc_char(b, '"');
    for (size_t i = 0; i < len && !b->overflow; i++) {
        uint8_t c = data[i];
        switch (c) {
        case '"':  enc_lit(b, "\\\""); break;
        case '\\': enc_lit(b, "\\\\"); break;
        case '\b': enc_lit(b, "\\b"); break;
        case '\f': enc_lit(b, "\\f"); break;
        case '\n': enc_lit(b, "\\n"); break;
        case '\r': enc_lit(b, "\\r"); break;
        case '\t': enc_lit(b, "\\t"); break;
        default:
            if (c < 0x20) {
                char esc[7];
                snprintf(esc, sizeof(esc), "\\u%04x", c);
                enc_raw(b, esc, 6);
            } else {
                enc_char(b, (char)c);
            }
        }
    }
    enc_char(b, '"');
}

static void enc_key(enc_buf_t *b, const char *key)
{
    enc_char(b, '"');
    enc_lit(b, key);
    enc_char(b, '"');
    enc_char(b, ':');
}

static void enc_key_string(enc_buf_t *b, const char *key,
                            moq_bytes_t val)
{
    enc_key(b, key);
    enc_json_string(b, val.data, val.len);
}

static void enc_u64_raw(enc_buf_t *b, uint64_t val)
{
    char num[24];
    snprintf(num, sizeof(num), "%llu", (unsigned long long)val);
    enc_lit(b, num);
}

static void enc_key_u64(enc_buf_t *b, const char *key, uint64_t val)
{
    enc_key(b, key);
    enc_u64_raw(b, val);
}

static void enc_key_u32(enc_buf_t *b, const char *key, uint32_t val)
{
    enc_key_u64(b, key, val);
}

static void enc_key_int(enc_buf_t *b, const char *key, int val)
{
    enc_key(b, key);
    char num[16];
    snprintf(num, sizeof(num), "%d", val);
    enc_lit(b, num);
}

static void enc_key_bool(enc_buf_t *b, const char *key, bool val)
{
    enc_key(b, key);
    enc_lit(b, val ? "true" : "false");
}

static void enc_key_framerate(enc_buf_t *b, const char *key,
                               uint64_t millis)
{
    enc_key(b, key);
    char num[32];
    uint64_t whole = millis / 1000;
    uint64_t frac = millis % 1000;
    if (frac == 0) {
        snprintf(num, sizeof(num), "%llu", (unsigned long long)whole);
    } else if (frac % 100 == 0) {
        snprintf(num, sizeof(num), "%llu.%llu",
            (unsigned long long)whole, (unsigned long long)(frac / 100));
    } else if (frac % 10 == 0) {
        snprintf(num, sizeof(num), "%llu.%02llu",
            (unsigned long long)whole, (unsigned long long)(frac / 10));
    } else {
        snprintf(num, sizeof(num), "%llu.%03llu",
            (unsigned long long)whole, (unsigned long long)frac);
    }
    enc_lit(b, num);
}

static void enc_comma(enc_buf_t *b, bool *first)
{
    if (!*first) enc_char(b, ',');
    *first = false;
}

static bool validate_bytes(moq_bytes_t v)
{
    return v.len == 0 || v.data != NULL;
}

/* Byte-span equality (two empty spans are equal regardless of data pointer). */
static bool bytes_eq(moq_bytes_t a, moq_bytes_t b)
{
    if (a.len != b.len) return false;
    if (a.len == 0) return true;
    return a.data && b.data && memcmp(a.data, b.data, a.len) == 0;
}

/* MSF §5.2.3 identity of a track within a catalog: a name is unique *per
 * namespace*, so two tracks collide only when both name and namespace match.
 * A track with no namespace and one with a namespace are in different
 * namespaces and never collide on name alone. */
static bool track_tuple_eq(const moq_msf_track_t *x, const moq_msf_track_t *y)
{
    if (!bytes_eq(x->name, y->name)) return false;
    if (x->has_namespace != y->has_namespace) return false;
    if (x->has_namespace && !bytes_eq(x->namespace_, y->namespace_))
        return false;
    return true;
}

static bool bytes_eq_lit(moq_bytes_t v, const char *lit)
{
    size_t n = strlen(lit);
    return v.len == n && v.data && memcmp(v.data, lit, n) == 0;
}

/* CMSF §4.1.1.2 / §4.1.1.4.1: a contentProtection UUID string is exactly
 * "8-4-4-4-12" hex digits with hyphens (defaultKID entries and drmSystem
 * systemID). Case-insensitive hex. Syntax only -- the UUID value/registry
 * meaning is not interpreted. Used on the ENCODE/authoring path only; decode
 * stays lenient for interop. */
static bool cp_is_uuid(moq_bytes_t v)
{
    if (!v.data || v.len != 36) return false;
    for (size_t i = 0; i < 36; i++) {
        uint8_t c = v.data[i];
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (c != '-') return false;
        } else if (!((c >= '0' && c <= '9') ||
                     (c >= 'a' && c <= 'f') ||
                     (c >= 'A' && c <= 'F'))) {
            return false;
        }
    }
    return true;
}

/* Validate the optional (has_*-gated) track spans and arrays. Shared by full
 * track validation and clone-op track validation. */
static moq_result_t validate_track_optionals(const moq_msf_track_t *t)
{
    if (t->has_namespace && !validate_bytes(t->namespace_)) return MOQ_ERR_INVAL;
    if (t->has_role && !validate_bytes(t->role)) return MOQ_ERR_INVAL;
    if (t->has_codec && !validate_bytes(t->codec)) return MOQ_ERR_INVAL;
    if (t->has_init_data && !validate_bytes(t->init_data)) return MOQ_ERR_INVAL;
    if (t->has_init_track && !validate_bytes(t->init_track)) return MOQ_ERR_INVAL;
    if (t->has_channel_config && !validate_bytes(t->channel_config)) return MOQ_ERR_INVAL;
    if (t->has_lang && !validate_bytes(t->lang)) return MOQ_ERR_INVAL;
    if (t->has_label && !validate_bytes(t->label)) return MOQ_ERR_INVAL;
    if (t->has_init_ref && !validate_bytes(t->init_ref)) return MOQ_ERR_INVAL;
    if (t->has_event_type && !validate_bytes(t->event_type)) return MOQ_ERR_INVAL;
    if (t->has_mime_type &&
        (!validate_bytes(t->mime_type) || t->mime_type.len == 0))
        return MOQ_ERR_INVAL;

    /* depends span coherence: a non-empty list must have a backing array and
     * every entry must be a valid non-empty string. */
    if (t->depends_count > 0 && !t->depends) return MOQ_ERR_INVAL;
    for (size_t i = 0; i < t->depends_count; i++)
        if (!validate_bytes(t->depends[i]) || t->depends[i].len == 0)
            return MOQ_ERR_INVAL;

    if (t->cp_ref_id_count > 0 && !t->cp_ref_ids) return MOQ_ERR_INVAL;
    for (size_t i = 0; i < t->cp_ref_id_count; i++)
        if (!validate_bytes(t->cp_ref_ids[i]) || t->cp_ref_ids[i].len == 0)
            return MOQ_ERR_INVAL;

    return MOQ_OK;
}

static moq_result_t validate_track(const moq_msf_track_t *t)
{
    if (!validate_bytes(t->name) || t->name.len == 0) return MOQ_ERR_INVAL;
    if (!validate_bytes(t->packaging) || t->packaging.len == 0) return MOQ_ERR_INVAL;

    moq_result_t rc = validate_track_optionals(t);
    if (rc < 0) return rc;

    /* MSF 5.2.35: trackDuration MUST NOT be present on a live track. */
    if (t->has_track_duration && t->is_live) return MOQ_ERR_INVAL;

    /* MSF 5.2.5: eventType is required iff packaging is "eventtimeline". */
    bool is_event_timeline = bytes_eq_lit(t->packaging, "eventtimeline");
    if (is_event_timeline &&
        (!t->has_event_type || t->event_type.len == 0)) return MOQ_ERR_INVAL;
    if (!is_event_timeline && t->has_event_type) return MOQ_ERR_INVAL;

    /* MSF §8.2: an eventtimeline track MUST carry mimeType "application/json"
     * and a non-empty depends list (the tracks it applies to). mimeType and
     * depends stay optional, free-form metadata for other packaging types. */
    if (is_event_timeline) {
        if (!t->has_mime_type ||
            !bytes_eq_lit(t->mime_type, "application/json")) return MOQ_ERR_INVAL;
        if (t->depends_count == 0) return MOQ_ERR_INVAL;
    }

    /* MSF §7.2: an explicit media timeline track (packaging "mediatimeline")
     * MUST carry mimeType "application/json" and a non-empty depends list
     * naming the tracks it applies to. (eventType is already forbidden above
     * for any non-eventtimeline packaging.) */
    if (bytes_eq_lit(t->packaging, "mediatimeline")) {
        if (!t->has_mime_type ||
            !bytes_eq_lit(t->mime_type, "application/json")) return MOQ_ERR_INVAL;
        if (t->depends_count == 0) return MOQ_ERR_INVAL;
    }

    return MOQ_OK;
}

/* Validate a delta op track per op kind (MSF 5.1.6). An "add" track is a full
 * track; a "remove" track carries only name (+optional namespace); a "clone"
 * track carries parentName + a new name + optional overrides. */
static moq_result_t validate_delta_track(moq_msf_delta_op_kind_t op,
                                         const moq_msf_track_t *t)
{
    if (!validate_bytes(t->name) || t->name.len == 0) return MOQ_ERR_INVAL;
    if (op == MOQ_MSF_DELTA_OP_ADD) {
        /* Lenient like the parser: packaging may be omitted in the example;
         * if present it must be non-empty. Optional spans must be coherent. */
        if (t->has_packaging &&
            (!validate_bytes(t->packaging) || t->packaging.len == 0))
            return MOQ_ERR_INVAL;
        return validate_track_optionals(t);
    }
    if (op == MOQ_MSF_DELTA_OP_REMOVE) {
        /* MSF 5.1.6: name (+optional namespace) only. */
        if (!track_is_name_namespace_only(t)) return MOQ_ERR_INVAL;
        if (t->has_namespace && !validate_bytes(t->namespace_)) return MOQ_ERR_INVAL;
        return MOQ_OK;
    }
    /* CLONE */
    if (!t->has_parent_name || !validate_bytes(t->parent_name) ||
        t->parent_name.len == 0) return MOQ_ERR_INVAL;
    if (t->has_parent_namespace && !validate_bytes(t->parent_namespace))
        return MOQ_ERR_INVAL;
    if (t->has_packaging &&
        (!validate_bytes(t->packaging) || t->packaging.len == 0))
        return MOQ_ERR_INVAL;
    return validate_track_optionals(t);
}

static moq_result_t validate_url(const moq_cmsf_url_t *u)
{
    if (!u->present) return MOQ_OK;
    if (!validate_bytes(u->url) || u->url.len == 0) return MOQ_ERR_INVAL;
    if (u->has_type && !validate_bytes(u->type)) return MOQ_ERR_INVAL;
    return MOQ_OK;
}

/* Validate one contentProtection entry. `strict` applies the CMSF §4 authoring
 * SYNTAX rules (scheme enum + UUID systemID/defaultKID); when false -- the
 * receive-path re-encode inside moq_msf_catalog_apply_delta -- only the shape
 * rules are applied so a leniently-parsed inbound catalog round-trips. */
static bool b64_syntax_ok(moq_bytes_t b);   /* defined with the Base64 helpers */

static moq_result_t validate_content_protection(
    const moq_cmsf_content_protection_t *cp, bool strict)
{
    if (!validate_bytes(cp->ref_id) || cp->ref_id.len == 0) return MOQ_ERR_INVAL;
    if (strict) {
        /* CMSF §4.1.1.3: scheme is the CENC enum "cenc" or "cbcs". */
        if (!(bytes_eq_lit(cp->scheme, "cenc") ||
              bytes_eq_lit(cp->scheme, "cbcs"))) return MOQ_ERR_INVAL;
    } else if (!validate_bytes(cp->scheme) || cp->scheme.len == 0) {
        return MOQ_ERR_INVAL;
    }
    if (cp->default_kid_count > 0 && !cp->default_kids) return MOQ_ERR_INVAL;
    for (size_t i = 0; i < cp->default_kid_count; i++) {
        if (strict) {
            /* CMSF §4.1.1.2: each defaultKID is a UUID string (syntax only). */
            if (!cp_is_uuid(cp->default_kids[i])) return MOQ_ERR_INVAL;
        } else if (!validate_bytes(cp->default_kids[i]) ||
                   cp->default_kids[i].len == 0) {
            return MOQ_ERR_INVAL;
        }
    }

    const moq_cmsf_drm_system_t *ds = &cp->drm_system;
    if (strict) {
        /* CMSF §4.1.1.4.1: drmSystem.systemID is a UUID string (syntax only). */
        if (!cp_is_uuid(ds->system_id)) return MOQ_ERR_INVAL;
    } else if (!validate_bytes(ds->system_id) || ds->system_id.len == 0) {
        return MOQ_ERR_INVAL;
    }
    moq_result_t rc;
    if ((rc = validate_url(&ds->la_url)) < 0) return rc;
    if ((rc = validate_url(&ds->cert_url)) < 0) return rc;
    if ((rc = validate_url(&ds->auth_url)) < 0) return rc;
    if (ds->has_pssh) {
        if (!validate_bytes(ds->pssh)) return MOQ_ERR_INVAL;
        /* CMSF §4.1.1.4.5: pssh is a Base64-encoded PSSH box. Strict authoring
         * requires a non-empty, syntactically valid Base64 string; the lenient
         * receive path keeps whatever the peer sent. (Box-shape validation is
         * intentionally out of scope -- it would need a CMAF/CENC parser.) */
        if (strict && (ds->pssh.len == 0 || !b64_syntax_ok(ds->pssh)))
            return MOQ_ERR_INVAL;
    }
    if (ds->has_robustness && !validate_bytes(ds->robustness)) return MOQ_ERR_INVAL;
    return MOQ_OK;
}

/* Emit the optional (has_*-gated) track fields, managing the leading comma via
 * *f. Does NOT emit name / packaging / isLive / parent fields. */
static void enc_track_optionals(enc_buf_t *b, const moq_msf_track_t *t, bool *f)
{
    if (t->has_namespace)      { enc_comma(b, f); enc_key_string(b, "namespace", t->namespace_); }
    if (t->has_role)           { enc_comma(b, f); enc_key_string(b, "role", t->role); }
    if (t->has_codec)          { enc_comma(b, f); enc_key_string(b, "codec", t->codec); }
    if (t->has_init_data)      { enc_comma(b, f); enc_key_string(b, "initData", t->init_data); }
    if (t->has_init_track)     { enc_comma(b, f); enc_key_string(b, "initTrack", t->init_track); }
    if (t->has_width)          { enc_comma(b, f); enc_key_u32(b, "width", t->width); }
    if (t->has_height)         { enc_comma(b, f); enc_key_u32(b, "height", t->height); }
    if (t->has_samplerate)     { enc_comma(b, f); enc_key_u32(b, "samplerate", t->samplerate); }
    if (t->has_bitrate)        { enc_comma(b, f); enc_key_u64(b, "bitrate", t->bitrate); }
    if (t->has_framerate)      { enc_comma(b, f); enc_key_framerate(b, "framerate", t->framerate_millis); }
    if (t->has_render_group)   { enc_comma(b, f); enc_key_int(b, "renderGroup", t->render_group); }
    if (t->has_alt_group)      { enc_comma(b, f); enc_key_int(b, "altGroup", t->alt_group); }
    if (t->has_timescale)      { enc_comma(b, f); enc_key_u64(b, "timescale", t->timescale); }
    if (t->has_target_latency) { enc_comma(b, f); enc_key_u64(b, "targetLatency", t->target_latency); }
    if (t->has_track_duration) { enc_comma(b, f); enc_key_u64(b, "trackDuration", t->track_duration_ms); }
    if (t->has_channel_config) { enc_comma(b, f); enc_key_string(b, "channelConfig", t->channel_config); }
    if (t->has_lang)           { enc_comma(b, f); enc_key_string(b, "lang", t->lang); }
    if (t->has_label)          { enc_comma(b, f); enc_key_string(b, "label", t->label); }
    if (t->has_init_ref)       { enc_comma(b, f); enc_key_string(b, "initRef", t->init_ref); }
    if (t->has_event_type)     { enc_comma(b, f); enc_key_string(b, "eventType", t->event_type); }
    if (t->has_mime_type)      { enc_comma(b, f); enc_key_string(b, "mimeType", t->mime_type); }
    if (t->has_max_grp_sap)    { enc_comma(b, f); enc_key_u32(b, "maxGrpSapStartingType", t->max_grp_sap); }
    if (t->has_max_obj_sap)    { enc_comma(b, f); enc_key_u32(b, "maxObjSapStartingType", t->max_obj_sap); }
    if (t->has_template) {
        /* MSF 5.2.15 / §7.4 fixed shape:
         * [startMediaTime, deltaMediaTime, [g,o], [dg,do], startWC, deltaWC]. */
        const moq_msf_media_template_t *tpl = &t->template_;
        enc_comma(b, f);
        enc_key(b, "template");
        enc_char(b, '[');
        enc_u64_raw(b, tpl->start_media_ms); enc_char(b, ',');
        enc_u64_raw(b, tpl->delta_media_ms); enc_char(b, ',');
        enc_char(b, '[');
        enc_u64_raw(b, tpl->start_group);  enc_char(b, ',');
        enc_u64_raw(b, tpl->start_object); enc_char(b, ']'); enc_char(b, ',');
        enc_char(b, '[');
        enc_u64_raw(b, tpl->delta_group);  enc_char(b, ',');
        enc_u64_raw(b, tpl->delta_object); enc_char(b, ']'); enc_char(b, ',');
        enc_u64_raw(b, tpl->start_wallclock_ms); enc_char(b, ',');
        enc_u64_raw(b, tpl->delta_wallclock_ms);
        enc_char(b, ']');
    }
    if (t->depends_count > 0) {
        enc_comma(b, f);
        enc_key(b, "depends");
        enc_char(b, '[');
        for (size_t i = 0; i < t->depends_count; i++) {
            if (i > 0) enc_char(b, ',');
            enc_json_string(b, t->depends[i].data, t->depends[i].len);
        }
        enc_char(b, ']');
    }
    if (t->cp_ref_id_count > 0) {
        enc_comma(b, f);
        enc_key(b, "contentProtectionRefIDs");
        enc_char(b, '[');
        for (size_t i = 0; i < t->cp_ref_id_count; i++) {
            if (i > 0) enc_char(b, ',');
            enc_json_string(b, t->cp_ref_ids[i].data, t->cp_ref_ids[i].len);
        }
        enc_char(b, ']');
    }
}

static void enc_track(enc_buf_t *b, const moq_msf_track_t *t)
{
    enc_char(b, '{');
    bool f = true;

    enc_comma(b, &f); enc_key_string(b, "name", t->name);
    enc_comma(b, &f); enc_key_string(b, "packaging", t->packaging);
    enc_comma(b, &f); enc_key_bool(b, "isLive", t->is_live);
    enc_track_optionals(b, t, &f);
    enc_char(b, '}');
}

/* Emit a delta op track object per op kind (MSF 5.1.6). */
static void enc_delta_op_track(enc_buf_t *b, moq_msf_delta_op_kind_t op,
                               const moq_msf_track_t *t)
{
    enc_char(b, '{');
    bool f = true;

    if (op == MOQ_MSF_DELTA_OP_REMOVE) {
        enc_comma(b, &f); enc_key_string(b, "name", t->name);
        if (t->has_namespace) { enc_comma(b, &f); enc_key_string(b, "namespace", t->namespace_); }
        enc_char(b, '}');
        return;
    }
    if (op == MOQ_MSF_DELTA_OP_CLONE) {
        enc_comma(b, &f); enc_key_string(b, "parentName", t->parent_name);
        if (t->has_parent_namespace) { enc_comma(b, &f); enc_key_string(b, "parentNamespace", t->parent_namespace); }
        enc_comma(b, &f); enc_key_string(b, "name", t->name);
        if (t->has_packaging) { enc_comma(b, &f); enc_key_string(b, "packaging", t->packaging); }
        if (t->has_is_live)   { enc_comma(b, &f); enc_key_bool(b, "isLive", t->is_live); }
        enc_track_optionals(b, t, &f);
        enc_char(b, '}');
        return;
    }
    /* ADD: a (possibly partial) track object; packaging/isLive emitted only
     * when present so a §5.6.4-style add round-trips. */
    enc_comma(b, &f); enc_key_string(b, "name", t->name);
    if (t->has_packaging) { enc_comma(b, &f); enc_key_string(b, "packaging", t->packaging); }
    if (t->has_is_live)   { enc_comma(b, &f); enc_key_bool(b, "isLive", t->is_live); }
    enc_track_optionals(b, t, &f);
    enc_char(b, '}');
}

/* -- Content protection encoding ------------------------------------- */

static void enc_url(enc_buf_t *b, const char *key, const moq_cmsf_url_t *u)
{
    enc_key(b, key);
    enc_char(b, '{');
    bool f = true;
    enc_comma(b, &f); enc_key_string(b, "url", u->url);
    if (u->has_type) { enc_comma(b, &f); enc_key_string(b, "type", u->type); }
    enc_char(b, '}');
}

static void enc_drm_system(enc_buf_t *b, const moq_cmsf_drm_system_t *ds)
{
    enc_key(b, "drmSystem");
    enc_char(b, '{');
    bool f = true;
    enc_comma(b, &f); enc_key_string(b, "systemID", ds->system_id);
    if (ds->la_url.present)   { enc_comma(b, &f); enc_url(b, "laURL", &ds->la_url); }
    if (ds->cert_url.present) { enc_comma(b, &f); enc_url(b, "certURL", &ds->cert_url); }
    if (ds->auth_url.present) { enc_comma(b, &f); enc_url(b, "authURL", &ds->auth_url); }
    if (ds->has_pssh)         { enc_comma(b, &f); enc_key_string(b, "pssh", ds->pssh); }
    if (ds->has_robustness)   { enc_comma(b, &f); enc_key_string(b, "robustness", ds->robustness); }
    enc_char(b, '}');
}

static void enc_content_protection(enc_buf_t *b,
                                   const moq_cmsf_content_protection_t *cp)
{
    enc_char(b, '{');
    bool f = true;
    enc_comma(b, &f); enc_key_string(b, "refID", cp->ref_id);
    enc_comma(b, &f);
    enc_key(b, "defaultKID");
    enc_char(b, '[');
    for (size_t i = 0; i < cp->default_kid_count; i++) {
        if (i > 0) enc_char(b, ',');
        enc_json_string(b, cp->default_kids[i].data, cp->default_kids[i].len);
    }
    enc_char(b, ']');
    enc_comma(b, &f); enc_key_string(b, "scheme", cp->scheme);
    enc_comma(b, &f); enc_drm_system(b, &cp->drm_system);
    enc_char(b, '}');
}

static const char *delta_op_name(moq_msf_delta_op_kind_t op)
{
    switch (op) {
    case MOQ_MSF_DELTA_OP_ADD:    return "add";
    case MOQ_MSF_DELTA_OP_REMOVE: return "remove";
    case MOQ_MSF_DELTA_OP_CLONE:  return "clone";
    }
    return "add";
}

static size_t enc_catalog_to(uint8_t *buf, size_t cap,
                              const moq_msf_catalog_t *cat)
{
    enc_buf_t b = { buf, cap, 0, false };

    /* MSF 5.1.6/5.3: a delta catalog carries no version/tracks, only the
     * deltaUpdate operations (with an optional generatedAt). */
    if (cat->delta_update) {
        enc_char(&b, '{');
        bool tf = true;
        if (cat->has_generated_at) {
            enc_comma(&b, &tf); enc_key_u64(&b, "generatedAt", cat->generated_at);
        }
        enc_comma(&b, &tf);
        enc_key(&b, "deltaUpdate");
        enc_char(&b, '[');
        for (size_t i = 0; i < cat->delta_update_count; i++) {
            const moq_msf_delta_op_t *op = &cat->delta_update[i];
            if (i > 0) enc_char(&b, ',');
            enc_char(&b, '{');
            enc_key_string(&b, "op",
                (moq_bytes_t){ (const uint8_t *)delta_op_name(op->op),
                               strlen(delta_op_name(op->op)) });
            enc_char(&b, ',');
            enc_key(&b, "tracks");
            enc_char(&b, '[');
            for (size_t j = 0; j < op->track_count; j++) {
                if (j > 0) enc_char(&b, ',');
                enc_delta_op_track(&b, op->op, &op->tracks[j]);
            }
            enc_char(&b, ']');
            enc_char(&b, '}');
        }
        enc_char(&b, ']');
        enc_char(&b, '}');
        if (buf && b.overflow) return 0;
        return b.pos;
    }

    /* MSF-01 §5.1.1: version is the JSON String "1" -- the numeral, not a
     * draft-name string. This is the wire form strict MSF-01 receivers
     * (PlayA) accept and the form libmoq itself emitted cross-impl before
     * the "draft-01" regression (E2E case D captured fixture). The parser
     * still accepts "draft-01" (catalogs from intermediate builds) and
     * legacy numeric 1 (MSF-00). */
    enc_lit(&b, "{\"version\":\"");
    { char num[8]; snprintf(num, sizeof(num), "%d", cat->version); enc_lit(&b, num); }
    enc_char(&b, '"');

    if (cat->has_generated_at) {
        enc_char(&b, ',');
        enc_key_u64(&b, "generatedAt", cat->generated_at);
    }

    /* MSF 5.1.3: isComplete is emitted only in its TRUE state (never false). */
    if (cat->is_complete) {
        enc_char(&b, ',');
        enc_key_bool(&b, "isComplete", true);
    }

    /* contentProtections is a root-level array (CMSF §4.1.1). */
    if (cat->content_protection_count > 0) {
        enc_lit(&b, ",\"contentProtections\":[");
        for (size_t i = 0; i < cat->content_protection_count; i++) {
            if (i > 0) enc_char(&b, ',');
            enc_content_protection(&b, &cat->content_protections[i]);
        }
        enc_char(&b, ']');
    }

    enc_lit(&b, ",\"tracks\":[");
    for (size_t i = 0; i < cat->track_count; i++) {
        if (i > 0) enc_char(&b, ',');
        enc_track(&b, &cat->tracks[i]);
    }
    enc_char(&b, ']');

    /* initDataList MUST follow the tracks array (MSF 5.1.7). */
    if (cat->init_data_count > 0) {
        enc_lit(&b, ",\"initDataList\":[");
        for (size_t i = 0; i < cat->init_data_count; i++) {
            const moq_msf_init_data_entry_t *e = &cat->init_data_list[i];
            if (i > 0) enc_char(&b, ',');
            enc_char(&b, '{');
            bool ef = true;
            enc_comma(&b, &ef); enc_key_string(&b, "id", e->id);
            enc_comma(&b, &ef); enc_key_string(&b, "type", e->type);
            enc_comma(&b, &ef); enc_key_string(&b, "data", e->data);
            enc_char(&b, '}');
        }
        enc_char(&b, ']');
    }

    enc_char(&b, '}');

    if (buf && b.overflow) return 0;
    return b.pos;
}

/* Public authoring entry: full CMSF §4 contentProtection syntax enforced. */
moq_result_t moq_msf_catalog_encode(const moq_alloc_t *alloc,
                                     const moq_msf_catalog_t *cat,
                                     moq_rcbuf_t **out_json)
{
    return catalog_encode_impl(alloc, cat, out_json, /*strict_cp=*/true);
}

static moq_result_t catalog_encode_impl(const moq_alloc_t *alloc,
                                        const moq_msf_catalog_t *cat,
                                        moq_rcbuf_t **out_json, bool strict_cp)
{
    if (!alloc || !cat || !out_json) return MOQ_ERR_INVAL;
    *out_json = NULL;

    /* A delta catalog (MSF 5.1.6/5.3): no version/tracks; >= 1 op, each with
     * >= 1 op-validated track. */
    if (cat->delta_update) {
        if (cat->track_count > 0 || cat->delta_update_count == 0)
            return MOQ_ERR_INVAL;
        /* MSF 5.1.3: a delta never carries completion. */
        if (cat->is_complete) return MOQ_ERR_INVAL;
        for (size_t i = 0; i < cat->delta_update_count; i++) {
            const moq_msf_delta_op_t *op = &cat->delta_update[i];
            /* Reject an unknown op kind before encoding it (API footgun for
             * callers building a catalog directly). */
            if (op->op != MOQ_MSF_DELTA_OP_ADD &&
                op->op != MOQ_MSF_DELTA_OP_REMOVE &&
                op->op != MOQ_MSF_DELTA_OP_CLONE) return MOQ_ERR_INVAL;
            if (op->track_count == 0 || !op->tracks) return MOQ_ERR_INVAL;
            for (size_t j = 0; j < op->track_count; j++) {
                moq_result_t rc = validate_delta_track(op->op, &op->tracks[j]);
                if (rc < 0) return rc;
            }
        }
        size_t dn = enc_catalog_to(NULL, 0, cat);
        if (dn == 0) return MOQ_ERR_INVAL;
        uint8_t *dbuf = (uint8_t *)alloc->alloc(dn, alloc->ctx);
        if (!dbuf) return MOQ_ERR_NOMEM;
        size_t dw = enc_catalog_to(dbuf, dn, cat);
        if (dw != dn) { alloc->free(dbuf, dn, alloc->ctx); return MOQ_ERR_INVAL; }
        moq_result_t rc = moq_rcbuf_create(alloc, dbuf, dn, out_json);
        alloc->free(dbuf, dn, alloc->ctx);
        return rc;
    }

    if (cat->version != MOQ_MSF_VERSION) return MOQ_ERR_INVAL;
    if (cat->track_count > 0 && !cat->tracks) return MOQ_ERR_INVAL;

    for (size_t i = 0; i < cat->track_count; i++) {
        moq_result_t rc = validate_track(&cat->tracks[i]);
        if (rc < 0) return rc;
    }
    /* MSF §5.2.3: a track name is unique per namespace within the catalog.
     * Authoring only (strict_cp) -- the lenient receive re-encode preserves
     * whatever the peer sent, matching the contentProtection refID rule below. */
    if (strict_cp)
        for (size_t i = 0; i < cat->track_count; i++)
            for (size_t j = i + 1; j < cat->track_count; j++)
                if (track_tuple_eq(&cat->tracks[i], &cat->tracks[j]))
                    return MOQ_ERR_INVAL;

    if (cat->init_data_count > 0 && !cat->init_data_list) return MOQ_ERR_INVAL;
    for (size_t i = 0; i < cat->init_data_count; i++) {
        const moq_msf_init_data_entry_t *e = &cat->init_data_list[i];
        if (!validate_bytes(e->id) || e->id.len == 0) return MOQ_ERR_INVAL;
        if (!validate_bytes(e->type) || e->type.len == 0) return MOQ_ERR_INVAL;
        if (!validate_bytes(e->data)) return MOQ_ERR_INVAL;
    }
    /* MSF §5.1.7 / CMSF §3.1: each initDataList id is unique within the
     * catalog. Authoring only (strict_cp), as for track tuples above. */
    if (strict_cp)
        for (size_t i = 0; i < cat->init_data_count; i++)
            for (size_t j = i + 1; j < cat->init_data_count; j++)
                if (bytes_eq(cat->init_data_list[i].id,
                             cat->init_data_list[j].id))
                    return MOQ_ERR_INVAL;

    if (cat->content_protection_count > 0 && !cat->content_protections)
        return MOQ_ERR_INVAL;
    for (size_t i = 0; i < cat->content_protection_count; i++) {
        moq_result_t rc =
            validate_content_protection(&cat->content_protections[i], strict_cp);
        if (rc < 0) return rc;
    }
    /* CMSF §4.1.1.1: each contentProtection refID is unique across the array
     * (information MUST NOT be duplicated). Authoring only -- the lenient
     * receive re-encode (apply_delta) preserves whatever the peer sent. */
    if (strict_cp)
        for (size_t i = 0; i < cat->content_protection_count; i++)
            for (size_t j = i + 1; j < cat->content_protection_count; j++) {
                moq_bytes_t a = cat->content_protections[i].ref_id;
                moq_bytes_t b = cat->content_protections[j].ref_id;
                if (a.len == b.len && a.data && b.data &&
                    memcmp(a.data, b.data, a.len) == 0)
                    return MOQ_ERR_INVAL;
            }

    /* Measure pass: buf=NULL, counts bytes needed. */
    size_t needed = enc_catalog_to(NULL, 0, cat);
    if (needed == 0) return MOQ_ERR_INVAL;

    uint8_t *buf = (uint8_t *)alloc->alloc(needed, alloc->ctx);
    if (!buf) return MOQ_ERR_NOMEM;

    size_t written = enc_catalog_to(buf, needed, cat);
    if (written != needed) {
        alloc->free(buf, needed, alloc->ctx);
        return MOQ_ERR_INVAL;
    }

    moq_result_t rc = moq_rcbuf_create(alloc, buf, needed, out_json);
    alloc->free(buf, needed, alloc->ctx);
    return rc;
}

/* -- Base64 ---------------------------------------------------------- */

static const uint8_t b64_enc[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static const uint8_t b64_dec[256] = {
    ['A']=0, ['B']=1, ['C']=2, ['D']=3, ['E']=4, ['F']=5, ['G']=6,
    ['H']=7, ['I']=8, ['J']=9, ['K']=10,['L']=11,['M']=12,['N']=13,
    ['O']=14,['P']=15,['Q']=16,['R']=17,['S']=18,['T']=19,['U']=20,
    ['V']=21,['W']=22,['X']=23,['Y']=24,['Z']=25,
    ['a']=26,['b']=27,['c']=28,['d']=29,['e']=30,['f']=31,['g']=32,
    ['h']=33,['i']=34,['j']=35,['k']=36,['l']=37,['m']=38,['n']=39,
    ['o']=40,['p']=41,['q']=42,['r']=43,['s']=44,['t']=45,['u']=46,
    ['v']=47,['w']=48,['x']=49,['y']=50,['z']=51,
    ['0']=52,['1']=53,['2']=54,['3']=55,['4']=56,['5']=57,['6']=58,
    ['7']=59,['8']=60,['9']=61,['+']=62,['/']=63,
};

static bool b64_valid_char(uint8_t c)
{
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
           (c >= '0' && c <= '9') || c == '+' || c == '/';
}

/* True iff `b` is syntactically valid Base64: length a multiple of 4, only
 * alphabet characters with canonical '=' padding (at most two, only at the end,
 * with the unused low bits zero). Mirrors the syntax checks in
 * moq_msf_decode_init_data without allocating or decoding. An empty span is
 * "valid syntax" (decodes to nothing); callers that require non-empty check len
 * separately. */
static bool b64_syntax_ok(moq_bytes_t b)
{
    if (b.len == 0) return true;
    if (!b.data || b.len % 4 != 0) return false;
    size_t pad = 0;
    if (b.data[b.len - 1] == '=') pad++;
    if (b.len >= 2 && b.data[b.len - 2] == '=') pad++;
    for (size_t i = 0; i < b.len; i++) {
        uint8_t c = b.data[i];
        if (c == '=') { if (i < b.len - pad) return false; }
        else if (!b64_valid_char(c)) return false;
    }
    if (pad == 2) { if (b64_dec[b.data[b.len - 3]] & 0x0F) return false; }
    else if (pad == 1) { if (b64_dec[b.data[b.len - 2]] & 0x03) return false; }
    return true;
}

moq_result_t moq_msf_decode_init_data(const moq_alloc_t *alloc,
                                       moq_bytes_t init_data_b64,
                                       moq_rcbuf_t **out_data)
{
    if (!alloc || !out_data) return MOQ_ERR_INVAL;
    *out_data = NULL;
    if (init_data_b64.len > 0 && !init_data_b64.data) return MOQ_ERR_INVAL;

    if (init_data_b64.len == 0)
        return moq_rcbuf_create(alloc, NULL, 0, out_data);

    size_t len = init_data_b64.len;
    if (len % 4 != 0) return MOQ_ERR_PROTO;
    if (len > SIZE_MAX / 3) return MOQ_ERR_INVAL;

    const uint8_t *src = init_data_b64.data;

    /* Count padding. */
    size_t pad = 0;
    if (src[len - 1] == '=') pad++;
    if (len >= 2 && src[len - 2] == '=') pad++;

    /* Validate entire input before allocating. */
    for (size_t i = 0; i < len; i++) {
        uint8_t c = src[i];
        if (c == '=') {
            if (i < len - pad) return MOQ_ERR_PROTO;
        } else if (!b64_valid_char(c)) {
            return MOQ_ERR_PROTO;
        }
    }

    /* Reject non-canonical padding (unused bits must be zero). */
    if (pad == 2) {
        uint8_t last_data = b64_dec[src[len - 3]];
        if (last_data & 0x0F) return MOQ_ERR_PROTO;
    } else if (pad == 1) {
        uint8_t last_data = b64_dec[src[len - 2]];
        if (last_data & 0x03) return MOQ_ERR_PROTO;
    }

    size_t out_len = (len / 4) * 3 - pad;
    uint8_t *buf = (uint8_t *)alloc->alloc(out_len, alloc->ctx);
    if (!buf) return MOQ_ERR_NOMEM;

    size_t si = 0, di = 0;
    while (si < len) {
        uint32_t s0 = b64_dec[src[si]];
        uint32_t s1 = b64_dec[src[si + 1]];
        uint32_t s2 = (src[si + 2] != '=') ? b64_dec[src[si + 2]] : 0;
        uint32_t s3 = (src[si + 3] != '=') ? b64_dec[src[si + 3]] : 0;
        uint32_t triple = (s0 << 18) | (s1 << 12) | (s2 << 6) | s3;
        if (di < out_len) buf[di++] = (uint8_t)(triple >> 16);
        if (di < out_len) buf[di++] = (uint8_t)(triple >> 8);
        if (di < out_len) buf[di++] = (uint8_t)triple;
        si += 4;
    }

    moq_result_t rc = moq_rcbuf_create(alloc, buf, out_len, out_data);
    alloc->free(buf, out_len, alloc->ctx);
    return rc;
}

moq_result_t moq_msf_encode_init_data(const moq_alloc_t *alloc,
                                       moq_bytes_t init_data,
                                       moq_rcbuf_t **out_b64)
{
    if (!alloc || !out_b64) return MOQ_ERR_INVAL;
    *out_b64 = NULL;
    if (init_data.len > 0 && !init_data.data) return MOQ_ERR_INVAL;

    if (init_data.len == 0)
        return moq_rcbuf_create(alloc, NULL, 0, out_b64);

    if (init_data.len > SIZE_MAX / 4 - 2) return MOQ_ERR_INVAL;
    size_t out_len = ((init_data.len + 2) / 3) * 4;

    uint8_t *buf = (uint8_t *)alloc->alloc(out_len, alloc->ctx);
    if (!buf) return MOQ_ERR_NOMEM;

    const uint8_t *src = init_data.data;
    size_t si = 0, di = 0;
    size_t full = (init_data.len / 3) * 3;

    while (si < full) {
        uint32_t triple = ((uint32_t)src[si] << 16) |
                           ((uint32_t)src[si+1] << 8) |
                            (uint32_t)src[si+2];
        buf[di++] = b64_enc[(triple >> 18) & 0x3F];
        buf[di++] = b64_enc[(triple >> 12) & 0x3F];
        buf[di++] = b64_enc[(triple >> 6)  & 0x3F];
        buf[di++] = b64_enc[triple & 0x3F];
        si += 3;
    }

    size_t rem = init_data.len - full;
    if (rem == 1) {
        uint32_t v = (uint32_t)src[si] << 16;
        buf[di++] = b64_enc[(v >> 18) & 0x3F];
        buf[di++] = b64_enc[(v >> 12) & 0x3F];
        buf[di++] = '=';
        buf[di++] = '=';
    } else if (rem == 2) {
        uint32_t v = ((uint32_t)src[si] << 16) | ((uint32_t)src[si+1] << 8);
        buf[di++] = b64_enc[(v >> 18) & 0x3F];
        buf[di++] = b64_enc[(v >> 12) & 0x3F];
        buf[di++] = b64_enc[(v >> 6) & 0x3F];
        buf[di++] = '=';
    }

    moq_result_t rc = moq_rcbuf_create(alloc, buf, out_len, out_b64);
    alloc->free(buf, out_len, alloc->ctx);
    return rc;
}

/* -- Media timeline (§7.1.1) explicit payload codec ------------------ *
 * A hand-rolled, zero-allocation cursor over the caller's bytes (the catalog
 * path uses the vendored JSON DOM, but media-timeline objects are decoded on a
 * hot path and into caller storage, so no DOM is built here). */

typedef struct { const char *p, *end; } mt_cur_t;

static void mt_ws(mt_cur_t *c)
{
    while (c->p < c->end) {
        char x = *c->p;
        if (x != ' ' && x != '\t' && x != '\n' && x != '\r') break;
        c->p++;
    }
}

/* Skip whitespace, then match and consume a single expected character. */
static bool mt_ch(mt_cur_t *c, char want)
{
    mt_ws(c);
    if (c->p >= c->end || *c->p != want) return false;
    c->p++;
    return true;
}

/* Skip whitespace, then parse one non-negative integral JSON number into *out.
 * Rejects a leading sign, a fractional part, and an exponent (same integrality
 * rule as the catalog template scanner) as well as u64 overflow. */
static bool mt_u64(mt_cur_t *c, uint64_t *out)
{
    mt_ws(c);
    if (c->p >= c->end || *c->p < '0' || *c->p > '9') return false;
    const char *start = c->p;
    uint64_t v = 0;
    while (c->p < c->end && *c->p >= '0' && *c->p <= '9') {
        unsigned d = (unsigned)(*c->p - '0');
        if (v > (UINT64_MAX - d) / 10u) return false;   /* overflow */
        v = v * 10u + d;
        c->p++;
    }
    /* Strict JSON number syntax: no leading zeros ("0" is fine, "01"/"00" not). */
    if (c->p - start > 1 && start[0] == '0') return false;
    /* A trailing '.', 'e'/'E', or sign means a non-integral JSON number. */
    if (c->p < c->end) {
        char x = *c->p;
        if (x == '.' || x == 'e' || x == 'E' || x == '+' || x == '-')
            return false;
    }
    *out = v;
    return true;
}

moq_result_t moq_msf_media_timeline_decode(
    moq_bytes_t json,
    moq_msf_media_timeline_record_t *out,
    size_t cap,
    size_t *out_count)
{
    if (!out_count) return MOQ_ERR_INVAL;
    *out_count = 0;
    if (!out && cap > 0) return MOQ_ERR_INVAL;
    /* NULL data is malformed (and avoids NULL pointer arithmetic below). */
    if (!json.data) return MOQ_ERR_PROTO;

    mt_cur_t c = { (const char *)json.data,
                   (const char *)json.data + json.len };

    if (!mt_ch(&c, '[')) return MOQ_ERR_PROTO;

    size_t n = 0;
    mt_ws(&c);
    if (c.p < c.end && *c.p == ']') {
        c.p++;   /* empty array: valid, no records */
    } else {
        for (;;) {
            moq_msf_media_timeline_record_t rec;
            if (!mt_ch(&c, '[')) return MOQ_ERR_PROTO;
            if (!mt_u64(&c, &rec.media_time_ms)) return MOQ_ERR_PROTO;
            if (!mt_ch(&c, ',')) return MOQ_ERR_PROTO;
            if (!mt_ch(&c, '[')) return MOQ_ERR_PROTO;   /* location array */
            if (!mt_u64(&c, &rec.group)) return MOQ_ERR_PROTO;
            if (!mt_ch(&c, ',')) return MOQ_ERR_PROTO;
            if (!mt_u64(&c, &rec.object)) return MOQ_ERR_PROTO;
            if (!mt_ch(&c, ']')) return MOQ_ERR_PROTO;   /* exactly 2 in location */
            if (!mt_ch(&c, ',')) return MOQ_ERR_PROTO;
            if (!mt_u64(&c, &rec.wallclock_ms)) return MOQ_ERR_PROTO;
            if (!mt_ch(&c, ']')) return MOQ_ERR_PROTO;   /* exactly 3 in record */

            if (n < cap) out[n] = rec;
            n++;

            if (mt_ch(&c, ',')) continue;
            if (mt_ch(&c, ']')) break;
            return MOQ_ERR_PROTO;
        }
    }

    /* Only trailing whitespace may follow the closing bracket. */
    mt_ws(&c);
    if (c.p != c.end) return MOQ_ERR_PROTO;

    *out_count = n;
    return (n > cap) ? MOQ_ERR_BUFFER : MOQ_OK;
}

/* Emit the compact explicit-array form into `b` (sizing pass when b->buf NULL). */
static void mt_emit(enc_buf_t *b,
                    const moq_msf_media_timeline_record_t *records,
                    size_t count)
{
    enc_char(b, '[');
    for (size_t i = 0; i < count; i++) {
        if (i) enc_char(b, ',');
        enc_char(b, '[');
        enc_u64_raw(b, records[i].media_time_ms);
        enc_char(b, ',');
        enc_char(b, '[');
        enc_u64_raw(b, records[i].group);
        enc_char(b, ',');
        enc_u64_raw(b, records[i].object);
        enc_char(b, ']');
        enc_char(b, ',');
        enc_u64_raw(b, records[i].wallclock_ms);
        enc_char(b, ']');
    }
    enc_char(b, ']');
}

moq_result_t moq_msf_media_timeline_encode(
    const moq_msf_media_timeline_record_t *records,
    size_t count,
    uint8_t *buf,
    size_t cap,
    size_t *out_len)
{
    if (!out_len) return MOQ_ERR_INVAL;
    *out_len = 0;
    if (!records && count > 0) return MOQ_ERR_INVAL;

    /* Sizing pass (no buffer): pos accumulates the exact required length. */
    enc_buf_t sz = { NULL, 0, 0, false };
    mt_emit(&sz, records, count);
    size_t needed = sz.pos;

    if (!buf || cap < needed) { *out_len = needed; return MOQ_ERR_BUFFER; }

    enc_buf_t b = { buf, cap, 0, false };
    mt_emit(&b, records, count);
    *out_len = b.pos;
    return MOQ_OK;
}
