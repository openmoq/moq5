/*
 * The MSF track supplement (MOQ_MSF_EXT_SUPPLEMENT): its initializer and
 * accessors, and how apply, the encoder and a receiver merge, judge and
 * compare it.
 */

#include <moq/msf.h>
#include "msf_internal.h"

#include <string.h>

_Static_assert(MOQ_MSF_TRACK_SUPPLEMENT_V0_SIZE ==
               offsetof(moq_msf_track_supplement_t, malformed) + sizeof(uint32_t),
               "the supplement's first published layout ends with malformed");

void moq_msf_track_supplement_init_sized(moq_msf_track_supplement_t *s,
                                         size_t s_size)
{
    if (!s || s_size < sizeof(uint32_t)) return;
    size_t n = s_size < sizeof(*s) ? s_size : sizeof(*s);
    memset(s, 0, n);
    uint32_t stamp = (uint32_t)n;
    memcpy(s, &stamp, sizeof(stamp));
}

const moq_msf_track_supplement_t *moq_msf_track_supplement(
    const moq_msf_track_t *track)
{
    return track ? moq_msf_ext_supplement(track->extensions,
                                          track->extension_count)
                 : NULL;
}

const moq_msf_track_supplement_t *moq_msf_supplement_of(
    const moq_msf_track_t *t)
{
    const moq_msf_track_supplement_t *s = moq_msf_track_supplement(t);
    return s && s->struct_size >= MOQ_MSF_TRACK_SUPPLEMENT_V0_SIZE ? s : NULL;
}

bool moq_msf_supplement_has_r02_fields(const moq_msf_track_supplement_t *s)
{
    return s && (s->has_packaging_metadata || s->has_keyframe_spacing ||
                 s->has_accessible_window_ms ||
                 s->depends_render_group_count > 0);
}

bool moq_msf_supplement_is_empty(const moq_msf_track_supplement_t *s)
{
    return !s->has_packaging_metadata && !s->has_keyframe_spacing &&
           !s->has_accessible_window_ms && !s->has_encryption_scheme &&
           !s->has_cipher_suite && !s->has_key_id && !s->has_track_base_key &&
           !s->has_depends && s->depends_render_group_count == 0 &&
           s->malformed == 0;
}

/* A clone or update op's supplement folded over the inherited one, field by
 * field. In MSF-02 a "depends" key on the op (has_depends) replaces both
 * lists; in MSF-01 an op's "depends" replaces the String list only when it
 * has entries, as it always has. Malformed bits are never inherited: apply
 * has judged them already. */
void moq_msf_supplement_merge(moq_msf_track_supplement_t *dst,
                              const moq_msf_track_supplement_t *o,
                              moq_msf_revision_t rev)
{
#define SUPP_OVERRIDE(has, f) if (o->has) { dst->has = true; dst->f = o->f; }
    SUPP_OVERRIDE(has_packaging_metadata, packaging_metadata)
    SUPP_OVERRIDE(has_keyframe_spacing, keyframe_spacing)
    SUPP_OVERRIDE(has_accessible_window_ms, accessible_window_ms)
    SUPP_OVERRIDE(has_encryption_scheme, encryption_scheme)
    SUPP_OVERRIDE(has_cipher_suite, cipher_suite)
    SUPP_OVERRIDE(has_key_id, key_id)
    SUPP_OVERRIDE(has_track_base_key, track_base_key)
#undef SUPP_OVERRIDE
    if (rev == MOQ_MSF_REVISION_02 && o->has_depends) {
        dst->depends_render_groups = o->depends_render_groups;
        dst->depends_render_group_count = o->depends_render_group_count;
    }
    dst->has_depends = false;
    dst->malformed = 0;
}

static bool span_ok(moq_bytes_t b) { return b.len == 0 || b.data != NULL; }

bool moq_msf_supplement_storage_ok(const moq_msf_track_supplement_t *s)
{
    if (s->has_packaging_metadata && !span_ok(s->packaging_metadata)) return false;
    if (s->has_encryption_scheme && !span_ok(s->encryption_scheme)) return false;
    if (s->has_cipher_suite && !span_ok(s->cipher_suite)) return false;
    if (s->has_key_id && !span_ok(s->key_id)) return false;
    if (s->has_track_base_key && !span_ok(s->track_base_key)) return false;
    if (s->depends_render_group_count > 0 && !s->depends_render_groups)
        return false;
    return true;
}

/* -- Packaging metadata (MSF-02 5.2.5) ---------------------------------------- */

static bool pm_char_ok(uint8_t c)
{
    return c != '=' && c != ';' && c != ' ' && c != '\t' && c != '\n' &&
           c != '\r' && c != '\v' && c != '\f';
}

/* Authoring: one or more "key=value" pairs separated by ';', each key and
 * value non-empty and free of whitespace, '=' and ';'. */
static bool pm_syntax_ok(moq_bytes_t m)
{
    if (m.len == 0 || !m.data) return false;
    size_t i = 0;
    for (;;) {
        size_t k = i;
        while (i < m.len && pm_char_ok(m.data[i])) i++;
        if (i == k || i >= m.len || m.data[i] != '=') return false;
        size_t v = ++i;
        while (i < m.len && pm_char_ok(m.data[i])) i++;
        if (i == v) return false;
        if (i == m.len) return true;
        if (m.data[i] != ';') return false;
        i++;
    }
}

bool moq_msf_packaging_metadata_find(moq_bytes_t metadata, const char *key,
                                     moq_bytes_t *out_value)
{
    if (!key || !out_value || (metadata.len > 0 && !metadata.data)) return false;
    size_t klen = strlen(key);
    size_t i = 0;
    while (i <= metadata.len) {
        size_t end = i;
        while (end < metadata.len && metadata.data[end] != ';') end++;
        /* The pair [i, end): its key runs to the first '='. */
        size_t eq = i;
        while (eq < end && metadata.data[eq] != '=') eq++;
        if (eq < end && eq - i == klen &&
            (klen == 0 || memcmp(metadata.data + i, key, klen) == 0)) {
            *out_value = (moq_bytes_t){ metadata.data + eq + 1, end - eq - 1 };
            return true;
        }
        if (end == metadata.len) break;
        i = end + 1;
    }
    return false;
}

static bool pkg_is_loc(moq_bytes_t p)
{
    return p.len == 3 && p.data && memcmp(p.data, "loc", 3) == 0;
}

static moq_msf_stream_mapping_t stream_mapping_of(moq_bytes_t v)
{
    if (v.len == 5 && memcmp(v.data, "group", 5) == 0)
        return MOQ_MSF_STREAM_MAPPING_GROUP;
    if (v.len == 6 && memcmp(v.data, "object", 6) == 0)
        return MOQ_MSF_STREAM_MAPPING_OBJECT;
    return MOQ_MSF_STREAM_MAPPING_UNRECOGNIZED;
}

moq_msf_stream_mapping_t moq_msf_track_stream_mapping(const moq_msf_track_t *track)
{
    if (!track || !pkg_is_loc(track->packaging))
        return MOQ_MSF_STREAM_MAPPING_GROUP;
    const moq_msf_track_supplement_t *s = moq_msf_supplement_of(track);
    moq_bytes_t v;
    if (!s || !s->has_packaging_metadata ||
        !moq_msf_packaging_metadata_find(s->packaging_metadata, "streamMapping",
                                         &v))
        return MOQ_MSF_STREAM_MAPPING_GROUP;
    return stream_mapping_of(v);
}

moq_result_t moq_msf_supplement_validate(const moq_msf_track_supplement_t *s,
                                         const moq_msf_track_t *t, bool strict,
                                         moq_msf_revision_t rev)
{
    (void)t;
    if (!moq_msf_supplement_storage_ok(s)) return MOQ_ERR_INVAL;
    if (!strict) return MOQ_OK;
    /* Authoring: no malformed naming key, and no MSF-02 field in an MSF-01
     * catalog (the encryption fields belong to both). */
    if (s->malformed) return MOQ_ERR_INVAL;
    if (rev == MOQ_MSF_REVISION_01 && moq_msf_supplement_has_r02_fields(s))
        return MOQ_ERR_INVAL;
    if (rev == MOQ_MSF_REVISION_02) {
        /* 5.2.46: a count of Objects. */
        if (s->has_keyframe_spacing && s->keyframe_spacing == 0)
            return MOQ_ERR_INVAL;
        /* 5.2.5 syntax; 4.1.1: a "loc" publisher chooses one of the two
         * modes (an op track without packaging is judged as "loc"). */
        if (s->has_packaging_metadata) {
            if (!pm_syntax_ok(s->packaging_metadata)) return MOQ_ERR_INVAL;
            bool loc = !t || !t->has_packaging || pkg_is_loc(t->packaging);
            moq_bytes_t v;
            if (loc && moq_msf_packaging_metadata_find(s->packaging_metadata,
                                                       "streamMapping", &v) &&
                stream_mapping_of(v) == MOQ_MSF_STREAM_MAPPING_UNRECOGNIZED)
                return MOQ_ERR_INVAL;
        }
    }
    return MOQ_OK;
}

static bool span_eq(moq_bytes_t a, moq_bytes_t b)
{
    return a.len == b.len &&
           (a.len == 0 || (a.data && b.data && memcmp(a.data, b.data, a.len) == 0));
}

bool moq_msf_supplement_equal(const moq_msf_track_supplement_t *a,
                              const moq_msf_track_supplement_t *b)
{
    if (!a || !b) return a == b;
#define SUPP_SPAN(has, f) \
    if (a->has != b->has || (a->has && !span_eq(a->f, b->f))) return false
#define SUPP_NUM(has, f) \
    if (a->has != b->has || (a->has && a->f != b->f)) return false
    SUPP_SPAN(has_packaging_metadata, packaging_metadata);
    SUPP_NUM(has_keyframe_spacing, keyframe_spacing);
    SUPP_NUM(has_accessible_window_ms, accessible_window_ms);
    SUPP_SPAN(has_encryption_scheme, encryption_scheme);
    SUPP_SPAN(has_cipher_suite, cipher_suite);
    SUPP_SPAN(has_key_id, key_id);
    SUPP_SPAN(has_track_base_key, track_base_key);
#undef SUPP_SPAN
#undef SUPP_NUM
    if (a->has_depends != b->has_depends || a->malformed != b->malformed ||
        a->depends_render_group_count != b->depends_render_group_count)
        return false;
    for (size_t i = 0; i < a->depends_render_group_count; i++)
        if (a->depends_render_groups[i] != b->depends_render_groups[i])
            return false;
    return true;
}

/* -- Required fields (MSF-02 5.2.4.1) --------------------------------------- */

static bool bytes_is(moq_bytes_t b, const char *lit)
{
    size_t n = strlen(lit);
    return b.len == n && b.data && memcmp(b.data, lit, n) == 0;
}

bool moq_msf_required_fields_ok(const moq_msf_track_t *t)
{
    /* Table 6; eventtimeline's eventType is required of every event
     * timeline, whatever its role, by 5.2.6. */
    if (!pkg_is_loc(t->packaging) || !t->has_role) return true;
    if (bytes_is(t->role, "audio"))
        return t->has_codec && t->has_bitrate && t->has_samplerate &&
               t->has_channel_config;
    if (bytes_is(t->role, "video"))
        return t->has_codec && t->has_bitrate && t->has_framerate;
    if (bytes_is(t->role, "audiodescription"))
        return t->has_codec && t->has_samplerate && t->has_channel_config &&
               t->has_lang;
    if (bytes_is(t->role, "signlanguage"))
        return t->has_codec && t->has_framerate;
    return true;
}

bool moq_msf_alt_group_windows_ok(const moq_msf_track_t *t, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        if (!t[i].has_alt_group) continue;
        const moq_msf_track_supplement_t *a = moq_msf_supplement_of(&t[i]);
        bool ah = a && a->has_accessible_window_ms;
        for (size_t j = i + 1; j < n; j++) {
            if (!t[j].has_alt_group || t[j].alt_group != t[i].alt_group) continue;
            const moq_msf_track_supplement_t *b = moq_msf_supplement_of(&t[j]);
            bool bh = b && b->has_accessible_window_ms;
            if (ah != bh || (ah && a->accessible_window_ms != b->accessible_window_ms))
                return false;
        }
    }
    return true;
}

/* -- Switch points (MSF-02 6.3.2) -------------------------------------------- */

moq_result_t moq_msf_switch_point_group(uint64_t group, uint64_t object,
                                        uint64_t source_spacing,
                                        uint64_t target_spacing,
                                        uint64_t *out_target_group)
{
    if (!out_target_group || source_spacing == 0 || target_spacing == 0 ||
        object >= source_spacing)
        return MOQ_ERR_INVAL;
    if (group > UINT64_MAX / source_spacing) return MOQ_ERR_INVAL;
    uint64_t pos = group * source_spacing;
    if (object > UINT64_MAX - pos) return MOQ_ERR_INVAL;
    pos += object;
    if (pos % target_spacing != 0) return MOQ_DONE;
    *out_target_group = pos / target_spacing;
    return MOQ_OK;
}

/* -- initDataList property types (MSF-02 5.1.7) -------------------------------- */

static bool hex_digit(uint8_t c, unsigned *v)
{
    if (c >= '0' && c <= '9') { *v = (unsigned)(c - '0'); return true; }
    if (c >= 'a' && c <= 'f') { *v = (unsigned)(c - 'a' + 10); return true; }
    if (c >= 'A' && c <= 'F') { *v = (unsigned)(c - 'A' + 10); return true; }
    return false;
}

/* 1..16 hex digits after an optional 0x/0X. */
static bool property_type_parse(moq_bytes_t d, uint64_t *out)
{
    size_t i = 0;
    if (d.len >= 2 && d.data[0] == '0' && (d.data[1] == 'x' || d.data[1] == 'X'))
        i = 2;
    size_t digits = d.len - i;
    if (digits == 0 || digits > 16) return false;
    uint64_t v = 0;
    for (; i < d.len; i++) {
        unsigned h;
        if (!hex_digit(d.data[i], &h)) return false;
        v = (v << 4) | h;
    }
    *out = v;
    return true;
}

static bool is_property_type(moq_bytes_t type)
{
    return bytes_is(type, "track-property") || bytes_is(type, "object-property");
}

moq_result_t moq_msf_init_data_property_type(
    const moq_msf_init_data_entry_t *entry, uint64_t *out_type)
{
    if (!entry || !out_type || !is_property_type(entry->type) ||
        (entry->data.len > 0 && !entry->data.data))
        return MOQ_ERR_INVAL;
    uint64_t v;
    if (!property_type_parse(entry->data, &v)) return MOQ_ERR_PROTO;
    *out_type = v;
    return MOQ_OK;
}

bool moq_msf_init_entry_authorable_r02(const moq_msf_init_data_entry_t *e)
{
    if (bytes_is(e->type, "inline")) return true;
    uint64_t v;
    return is_property_type(e->type) && property_type_parse(e->data, &v);
}
