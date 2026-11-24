#define JSON_H_IMPLEMENTATION
#include "../vendor/json.h"

#include <moq/msf.h>
#include <moq/rcbuf.h>
#include "msf_internal.h"
#include <errno.h>
#include <float.h>
#include <limits.h>
#include <math.h>
#include <stddef.h>
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

/* json.h makes one allocation for the whole DOM. The bridge prepends the
 * catalog's header (msf_dom_hdr_t) to it, so the block a parse owns is
 * [header | DOM], and every span still lies inside [_dom, _dom + _dom_size). */
typedef struct {
    const moq_alloc_t *alloc;
    size_t             total;   /* bytes of the block, header included */
    uint8_t           *base;    /* the block */
} alloc_ctx_t;

/* The header's share of the block: a multiple of every alignment the DOM
 * needs, so the DOM keeps the allocator's alignment. */
#define MSF_DOM_HDR_BYTES 16u
#define MSF_DOM_MAGIC     UINT32_C(0x4d534632)

_Static_assert(MSF_DOM_HDR_BYTES >= sizeof(msf_dom_hdr_t) &&
               MSF_DOM_HDR_BYTES % _Alignof(void *) == 0 &&
               MSF_DOM_HDR_BYTES % _Alignof(size_t) == 0 &&
               MSF_DOM_HDR_BYTES % _Alignof(uint64_t) == 0,
               "the DOM after the header stays aligned");

static void *json_alloc_bridge(void *user, size_t size)
{
    alloc_ctx_t *ctx = (alloc_ctx_t *)user;
    /* A second allocation would have no owner: refuse it. */
    if (ctx->base || size > SIZE_MAX - MSF_DOM_HDR_BYTES) return NULL;
    size_t total = size + MSF_DOM_HDR_BYTES;
    uint8_t *p = (uint8_t *)ctx->alloc->alloc(total, ctx->alloc->ctx);
    if (!p) return NULL;
    memset(p, 0, MSF_DOM_HDR_BYTES);
    ctx->base = p;
    ctx->total = total;
    return p + MSF_DOM_HDR_BYTES;
}

const msf_dom_hdr_t *moq_msf_dom_hdr(const moq_msf_catalog_t *cat)
{
    if (!cat || !cat->_dom || cat->_dom_size < MSF_DOM_HDR_BYTES) return NULL;
    const msf_dom_hdr_t *h = (const msf_dom_hdr_t *)cat->_dom;
    return h->magic == MSF_DOM_MAGIC ? h : NULL;
}

/* -- Catalog extents ------------------------------------------------- */

_Static_assert(MOQ_MSF_CATALOG_V0_SIZE == offsetof(moq_msf_catalog_t, version_form),
               "the first published layout ends where version_form starts");
_Static_assert(offsetof(moq_msf_catalog_t, revision) ==
               MOQ_MSF_CATALOG_SIZE_THROUGH_VERSION_FORM + 4,
               "revision lies past the slot that covers version_form's padding");
_Static_assert(offsetof(moq_msf_catalog_t, publish_tracks) ==
               offsetof(moq_msf_catalog_t, _reserved_publish_tracks) + 4,
               "publish_tracks follows its reserved slot");

#define MSF_CATALOG_SIZE_THROUGH_REVISION \
    (offsetof(moq_msf_catalog_t, revision) + sizeof(moq_msf_revision_t))

msf_extent_t moq_msf_extent_of(size_t size)
{
    if (size < MOQ_MSF_CATALOG_V0_SIZE) return MSF_EXTENT_SHORT;
    if (size < MSF_CATALOG_SIZE_THROUGH_REVISION) return MSF_EXTENT_R01;
    if (size < MOQ_MSF_CATALOG_SIZE_THROUGH_PUBLISH_TRACKS)
        return MSF_EXTENT_PARTIAL;
    return MSF_EXTENT_FULL;
}

moq_msf_revision_t moq_msf_catalog_revision_of(const moq_msf_catalog_t *cat)
{
    return moq_msf_extent_of(cat->struct_size) == MSF_EXTENT_FULL
               ? cat->revision : MOQ_MSF_REVISION_01;
}

size_t moq_msf_publish_tracks_of(const moq_msf_catalog_t *cat,
                                 const moq_msf_track_t **tracks)
{
    if (moq_msf_extent_of(cat->struct_size) != MSF_EXTENT_FULL) {
        *tracks = NULL;
        return 0;
    }
    *tracks = cat->publish_tracks;
    return cat->publish_track_count;
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

/* Whether the caller's struct covers version_form (a struct that ends at
 * `_dom_size` predates the field), and the form it selects. */
static bool catalog_has_version_form(const moq_msf_catalog_t *cat)
{
    return cat->struct_size >= offsetof(moq_msf_catalog_t, version_form) +
                                   sizeof(cat->version_form);
}

static moq_msf_version_form_t catalog_version_form(const moq_msf_catalog_t *cat)
{
    return catalog_has_version_form(cat) ? cat->version_form
                                         : MOQ_MSF_VERSION_FORM_NUMERAL;
}

/*
 * MSF version handling (§5.1.1). The encoder emits the JSON String "1" (the
 * form existing MSF-01 receivers accept) or, when the caller selects
 * MOQ_MSF_VERSION_FORM_DRAFT_NAME, "draft-01" (5.1.1's convention for
 * catalogs against an Internet-Draft release). The parser accepts both and the
 * pre-01 numeric form (1) for LEGACY COMPATIBILITY ONLY (MSF-00 emitted a JSON
 * number), and reports the form in *form. Anything else -- other versions,
 * other or malformed draft strings, empty, or a non-string/non-number -- is
 * unsupported; per §5.1.1 a subscriber MUST NOT parse a version it does not
 * understand.
 */
static bool version_is_supported(const json_object_element_t *e,
                                 bool allow_r02,
                                 moq_msf_version_form_t *form,
                                 moq_msf_revision_t *rev)
{
    json_string_t *s = json_value_as_string(e->value);
    *rev = MOQ_MSF_REVISION_01;
    if (s) {
        if (str_eq(s, "1")) { *form = MOQ_MSF_VERSION_FORM_NUMERAL; return true; }
        if (str_eq(s, "draft-01")) {
            *form = MOQ_MSF_VERSION_FORM_DRAFT_NAME;
            return true;
        }
        /* MSF-02 5.1.1: "draft-02", understood only by a caller whose
         * catalog struct holds the MSF-02 block. */
        if (allow_r02 && str_eq(s, "draft-02")) {
            *form = MOQ_MSF_VERSION_FORM_DRAFT_NAME;
            *rev = MOQ_MSF_REVISION_02;
            return true;
        }
        return false;
    }
    int v = 0;
    if (get_int(e, &v) && v == MOQ_MSF_VERSION) {   /* legacy MSF-00 numeric */
        *form = MOQ_MSF_VERSION_FORM_NUMERAL;
        return true;
    }
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

/* A parsed track's extension list and its records: one block per track that
 * carries any, allocated on its first record and freed with the track. The
 * entries come first, so a parsed track's list pointer is its block. */
#define MSF_EXT_ROWS       2
#define MSF_ROW_MPEG2TS    0u
#define MSF_ROW_SUPPLEMENT 1u
typedef struct msf_ext_block {
    moq_msf_track_ext_t        entries[MSF_EXT_ROWS];  /* ascending kind */
    uint32_t                   present;     /* bit r: moq_msf_ext_kinds[r] */
    moq_msf_mpeg2ts_t          ts;          /* row 0 */
    moq_msf_track_supplement_t supplement;  /* row 1 */
} msf_ext_block_t;

static void *block_record(msf_ext_block_t *b, size_t row)
{
    return row == 0 ? (void *)&b->ts : (void *)&b->supplement;
}

/* Point an entry at a record of its kind. */
static void ext_entry_set(moq_msf_track_ext_t *e, moq_msf_ext_kind_t kind,
                          const void *rec)
{
    e->kind = kind;
    if (kind == MOQ_MSF_EXT_SUPPLEMENT)
        e->u.supplement = (const moq_msf_track_supplement_t *)rec;
    else
        e->u.mpeg2ts = (const moq_msf_mpeg2ts_t *)rec;
}

const void *moq_msf_ext_record(const moq_msf_track_ext_t *e)
{
    switch (e->kind) {
    case MOQ_MSF_EXT_MPEG2TS:    return e->u.mpeg2ts;
    case MOQ_MSF_EXT_SUPPLEMENT: return e->u.supplement;
    default:                     return NULL;
    }
}

/* Rebuild a block's list from its present records, in kind order. */
static void block_relist(msf_ext_block_t *b, moq_msf_track_t *t)
{
    size_t k = 0;
    for (size_t r = 0; r < MSF_EXT_ROWS; r++)
        if (b->present & (1u << r))
            ext_entry_set(&b->entries[k++], moq_msf_ext_kinds[r].kind,
                          block_record(b, r));
    t->extension_count = k;
}

/* moq_msf_mpeg2ts_t as first published: through `malformed`. */
#define MSF_TS_V0 (offsetof(moq_msf_mpeg2ts_t, malformed) + sizeof(uint32_t))

_Static_assert(MOQ_MSF_MPEG2TS_V0_SIZE == MSF_TS_V0, "msf ts record floor");

void moq_msf_mpeg2ts_init_sized(moq_msf_mpeg2ts_t *m, size_t m_size)
{
    if (!m || m_size < sizeof(uint32_t)) return;
    size_t n = m_size < sizeof(*m) ? m_size : sizeof(*m);
    memset(m, 0, n);
    uint32_t stamp = (uint32_t)n;
    memcpy(m, &stamp, sizeof(stamp));
}

const moq_msf_track_ext_t *moq_msf_ext_find(const moq_msf_track_ext_t *list,
                                            size_t count,
                                            moq_msf_ext_kind_t kind)
{
    if (!list) return NULL;
    for (size_t i = 0; i < count; i++)
        if (list[i].kind == kind) return &list[i];
    return NULL;
}

const moq_msf_mpeg2ts_t *moq_msf_track_mpeg2ts(const moq_msf_track_t *track)
{
    return track ? moq_msf_ext_mpeg2ts(track->extensions, track->extension_count)
                 : NULL;
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
    if (t->extensions) {
        /* A parsed track's list is one parse block (msf_ext_block_t). */
        msf_ext_block_t *b = (msf_ext_block_t *)(uintptr_t)t->extensions;
        for (size_t r = 0; r < MSF_EXT_ROWS; r++)
            if (b->present & (1u << r))
                moq_msf_ext_kinds[r].free_owned(alloc, block_record(b, r));
        alloc->free(b, sizeof(*b), alloc->ctx);
        t->extensions = NULL;
        t->extension_count = 0;
    }
}

/* -- mpeg2ts track fields (draft-gregoire-moq-msfts) ------------------ */

static const char *const MPEG2TS_MODE_NAMES[] = {
    "", "unmodified-program", "unmodified-multiplex", "per-program",
    "es-packets",
};

static const char *const MPEG2TS_TS_MODE_NAMES[] = {
    "", "arrival-time", "opaque",
};

const char *moq_msf_mpeg2ts_mode_name(moq_msf_mpeg2ts_mode_t mode)
{
    if ((unsigned)mode >= sizeof(MPEG2TS_MODE_NAMES) / sizeof(MPEG2TS_MODE_NAMES[0]))
        return "";
    return MPEG2TS_MODE_NAMES[mode];
}

const char *moq_msf_mpeg2ts_timestamp_mode_name(moq_msf_mpeg2ts_timestamp_mode_t m)
{
    if ((unsigned)m >= sizeof(MPEG2TS_TS_MODE_NAMES) / sizeof(MPEG2TS_TS_MODE_NAMES[0]))
        return "";
    return MPEG2TS_TS_MODE_NAMES[m];
}

/* Mode names are matched exactly (case-sensitive). */
static moq_msf_mpeg2ts_mode_t mpeg2ts_mode_from(moq_bytes_t name)
{
    for (unsigned i = 1; i < sizeof(MPEG2TS_MODE_NAMES) / sizeof(MPEG2TS_MODE_NAMES[0]); i++) {
        size_t n = strlen(MPEG2TS_MODE_NAMES[i]);
        if (name.len == n && memcmp(name.data, MPEG2TS_MODE_NAMES[i], n) == 0)
            return (moq_msf_mpeg2ts_mode_t)i;
    }
    return MOQ_MSF_MPEG2TS_MODE_UNRECOGNIZED;
}

static moq_msf_mpeg2ts_timestamp_mode_t mpeg2ts_ts_mode_from(moq_bytes_t name)
{
    for (unsigned i = 1; i < sizeof(MPEG2TS_TS_MODE_NAMES) / sizeof(MPEG2TS_TS_MODE_NAMES[0]); i++) {
        size_t n = strlen(MPEG2TS_TS_MODE_NAMES[i]);
        if (name.len == n && memcmp(name.data, MPEG2TS_TS_MODE_NAMES[i], n) == 0)
            return (moq_msf_mpeg2ts_timestamp_mode_t)i;
    }
    return MOQ_MSF_MPEG2TS_TIMESTAMP_UNRECOGNIZED;
}

/* The exact value of a JSON number token (RFC 8259 grammar: int, optional
 * fraction, optional exponent) when it is a non-negative integer no larger
 * than max, whatever its spelling: 188, 188.0, 1.88e2 and 18800e-2 are all
 * 188. No floating point is involved, so no rounding can admit a value out
 * of range or a fractional one. */
static bool json_uint_exact(const char *s, size_t n, uint64_t max,
                            uint64_t *out)
{
    size_t i = 0;
    if (i >= n || s[i] < '0' || s[i] > '9') return false;     /* no sign */
    /* Significant digits of int and fraction, and how many were fraction. */
    const char *digits[2] = { NULL, NULL };
    size_t dlen[2] = { 0, 0 };
    digits[0] = s + i;
    while (i < n && s[i] >= '0' && s[i] <= '9') { i++; dlen[0]++; }
    if (i < n && s[i] == '.') {
        i++;
        digits[1] = s + i;
        while (i < n && s[i] >= '0' && s[i] <= '9') { i++; dlen[1]++; }
        if (dlen[1] == 0) return false;
    }
    long long exp10 = 0;
    if (i < n && (s[i] == 'e' || s[i] == 'E')) {
        i++;
        bool neg = false;
        if (i < n && (s[i] == '+' || s[i] == '-')) neg = s[i++] == '-';
        if (i >= n || s[i] < '0' || s[i] > '9') return false;
        while (i < n && s[i] >= '0' && s[i] <= '9') {
            if (exp10 < 100000) exp10 = exp10 * 10 + (s[i] - '0');
            i++;
        }
        if (neg) exp10 = -exp10;
    }
    if (i != n) return false;
    /* value = D * 10^(exp10 - dlen[1]) with D the digit string int||frac. */
    long long shift = exp10 - (long long)dlen[1];
    size_t total = dlen[0] + dlen[1];
    /* The digits that the shift moves behind the decimal point must be 0. */
    size_t keep = total;
    if (shift < 0) {
        size_t drop = (size_t)(-shift) < total ? (size_t)(-shift) : total;
        for (size_t k = total - drop; k < total; k++) {
            char c = k < dlen[0] ? digits[0][k] : digits[1][k - dlen[0]];
            if (c != '0') return false;
        }
        keep = total - drop;
        shift = 0;
    }
    uint64_t v = 0;
    for (size_t k = 0; k < keep; k++) {
        char c = k < dlen[0] ? digits[0][k] : digits[1][k - dlen[0]];
        uint64_t d = (uint64_t)(c - '0');
        if (v > (max - d) / 10) return false;
        v = v * 10 + d;
    }
    for (long long k = 0; k < shift && v != 0; k++) {
        if (v > max / 10) return false;
        v *= 10;
    }
    if (v > max) return false;
    *out = v;
    return true;
}

/* A JSON number holding a non-negative integer no larger than max. */
static bool mpeg2ts_num(const json_value_t *v, uint64_t max, uint64_t *out)
{
    json_number_t *n = json_value_as_number((json_value_t *)v);
    if (!n || n->number_size == 0) return false;
    return json_uint_exact(n->number, n->number_size, max, out);
}

/* Record one "mpeg2ts*" key. Returns false for a key this document does not
 * define (left for the caller to ignore), MOQ_ERR_NOMEM via *rc. */
static bool parse_mpeg2ts_key(const moq_alloc_t *alloc,
                              const json_object_element_t *e,
                              moq_msf_mpeg2ts_t *m, moq_result_t *rc)
{
    const json_value_t *v = e->value;
    uint64_t x = 0;
    *rc = MOQ_OK;
    if (str_eq(e->name, "mpeg2tsMode")) {
        m->has_mode = true;
        m->mode_name = (moq_bytes_t){ NULL, 0 };
        m->mode = MOQ_MSF_MPEG2TS_MODE_UNRECOGNIZED;
        m->malformed &= ~MOQ_MSF_MPEG2TS_F_MODE;
        if (get_string(e, &m->mode_name)) m->mode = mpeg2ts_mode_from(m->mode_name);
        else m->malformed |= MOQ_MSF_MPEG2TS_F_MODE;
    } else if (str_eq(e->name, "mpeg2tsPacketSize")) {
        m->has_packet_size = true;
        m->malformed &= ~MOQ_MSF_MPEG2TS_F_PACKET_SIZE;
        if (mpeg2ts_num(v, UINT32_MAX, &x)) m->packet_size = (uint32_t)x;
        else m->malformed |= MOQ_MSF_MPEG2TS_F_PACKET_SIZE;
    } else if (str_eq(e->name, "mpeg2tsEsPid")) {
        m->has_es_pid = true;
        m->malformed &= ~MOQ_MSF_MPEG2TS_F_ES_PID;
        if (mpeg2ts_num(v, UINT32_MAX, &x)) m->es_pid = (uint32_t)x;
        else m->malformed |= MOQ_MSF_MPEG2TS_F_ES_PID;
    } else if (str_eq(e->name, "mpeg2tsProgramNumber")) {
        m->has_program_number = true;
        m->malformed &= ~MOQ_MSF_MPEG2TS_F_PROGRAM_NUMBER;
        if (mpeg2ts_num(v, UINT32_MAX, &x)) m->program_number = (uint32_t)x;
        else m->malformed |= MOQ_MSF_MPEG2TS_F_PROGRAM_NUMBER;
    } else if (str_eq(e->name, "mpeg2tsPcrPid")) {
        m->has_pcr_pid = true;
        m->malformed &= ~MOQ_MSF_MPEG2TS_F_PCR_PID;
        if (mpeg2ts_num(v, UINT32_MAX, &x)) m->pcr_pid = (uint32_t)x;
        else m->malformed |= MOQ_MSF_MPEG2TS_F_PCR_PID;
    } else if (str_eq(e->name, "mpeg2tsMuxRate")) {
        m->has_mux_rate = true;
        m->malformed &= ~MOQ_MSF_MPEG2TS_F_MUX_RATE;
        if (mpeg2ts_num(v, UINT64_MAX, &x)) m->mux_rate = x;
        else m->malformed |= MOQ_MSF_MPEG2TS_F_MUX_RATE;
    } else if (str_eq(e->name, "mpeg2tsSiPids")) {
        if (m->si_pids) {           /* duplicate key: the last one wins */
            alloc->free(m->si_pids, m->si_pid_count * sizeof(uint32_t), alloc->ctx);
            m->si_pids = NULL;
        }
        m->si_pid_count = 0;
        m->has_si_pids = true;
        m->malformed &= ~MOQ_MSF_MPEG2TS_F_SI_PIDS;
        json_array_t *a = json_value_as_array((json_value_t *)v);
        if (!a) {
            m->malformed |= MOQ_MSF_MPEG2TS_F_SI_PIDS;
            return true;
        }
        if (a->length == 0) return true;
        size_t bytes;
        if (!moq_msf_checked_array_bytes(a->length, sizeof(uint32_t), &bytes)) {
            *rc = MOQ_ERR_NOMEM;
            return true;
        }
        uint32_t *arr = (uint32_t *)alloc->alloc(bytes, alloc->ctx);
        if (!arr) { *rc = MOQ_ERR_NOMEM; return true; }
        size_t n = 0;
        for (json_array_element_t *ae = a->start; ae && n < a->length; ae = ae->next) {
            if (mpeg2ts_num(ae->value, UINT32_MAX, &x)) {
                arr[n++] = (uint32_t)x;
            } else {
                arr[n++] = 0;
                m->malformed |= MOQ_MSF_MPEG2TS_F_SI_PIDS;
            }
        }
        m->si_pids = arr;
        m->si_pid_count = n;
    } else if (str_eq(e->name, "mpeg2tsRandomAccess")) {
        m->has_random_access = true;
        m->malformed &= ~MOQ_MSF_MPEG2TS_F_RANDOM_ACCESS;
        if (!get_bool(e, &m->random_access))
            m->malformed |= MOQ_MSF_MPEG2TS_F_RANDOM_ACCESS;
    } else if (str_eq(e->name, "mpeg2tsTimestampMode")) {
        m->has_timestamp_mode = true;
        m->timestamp_mode_name = (moq_bytes_t){ NULL, 0 };
        m->timestamp_mode = MOQ_MSF_MPEG2TS_TIMESTAMP_UNRECOGNIZED;
        m->malformed &= ~MOQ_MSF_MPEG2TS_F_TIMESTAMP_MODE;
        if (get_string(e, &m->timestamp_mode_name))
            m->timestamp_mode = mpeg2ts_ts_mode_from(m->timestamp_mode_name);
        else
            m->malformed |= MOQ_MSF_MPEG2TS_F_TIMESTAMP_MODE;
    } else if (str_eq(e->name, "mpeg2tsScte35Pid")) {
        m->has_scte35_pid = true;
        m->malformed &= ~MOQ_MSF_MPEG2TS_F_SCTE35_PID;
        if (mpeg2ts_num(v, UINT32_MAX, &x)) m->scte35_pid = (uint32_t)x;
        else m->malformed |= MOQ_MSF_MPEG2TS_F_SCTE35_PID;
    } else {
        return false;
    }
    return true;
}

/* A track's mpeg2ts record, or NULL when it carries none. */
static const moq_msf_mpeg2ts_t *track_ts(const moq_msf_track_t *t)
{
    return moq_msf_track_mpeg2ts(t);
}

/* Is this one of the "mpeg2ts*" keys the draft defines? */
static bool mpeg2ts_key_known(const json_string_t *name)
{
    static const char *const keys[] = {
        "mpeg2tsMode", "mpeg2tsPacketSize", "mpeg2tsEsPid",
        "mpeg2tsProgramNumber", "mpeg2tsPcrPid", "mpeg2tsMuxRate",
        "mpeg2tsSiPids", "mpeg2tsRandomAccess", "mpeg2tsTimestampMode",
        "mpeg2tsScte35Pid",
    };
    for (size_t i = 0; i < sizeof(keys) / sizeof(keys[0]); i++)
        if (str_eq(name, keys[i])) return true;
    return false;
}

/* The record of registry row `row` on a track being parsed, allocated with
 * the track's block on first use. NULL on allocation failure. */
static void *parse_ext_rec(const moq_alloc_t *alloc, moq_msf_track_t *t,
                           size_t row)
{
    msf_ext_block_t *b = (msf_ext_block_t *)(uintptr_t)t->extensions;
    if (!b) {
        b = (msf_ext_block_t *)alloc->alloc(sizeof(*b), alloc->ctx);
        if (!b) return NULL;
        memset(b, 0, sizeof(*b));
        t->extensions = &b->entries[0];
        t->extension_count = 0;
    }
    void *rec = block_record(b, row);
    if (!(b->present & (1u << row))) {
        moq_msf_ext_kinds[row].init(rec);
        b->present |= 1u << row;
        block_relist(b, t);
    }
    return rec;
}

/* After a track's keys: drop records that ended up holding nothing (a key
 * whose value was malformed and so absent), and the block with them, so a
 * track carries exactly the records it has fields for. */
static void parse_ext_finish(const moq_alloc_t *alloc, moq_msf_track_t *t)
{
    msf_ext_block_t *b = (msf_ext_block_t *)(uintptr_t)t->extensions;
    if (!b) return;
    for (size_t r = 0; r < MSF_EXT_ROWS; r++) {
        if ((b->present & (1u << r)) &&
            moq_msf_ext_kinds[r].is_empty(block_record(b, r))) {
            moq_msf_ext_kinds[r].free_owned(alloc, block_record(b, r));
            b->present &= ~(1u << r);
        }
    }
    if (!b->present) {
        alloc->free(b, sizeof(*b), alloc->ctx);
        t->extensions = NULL;
        t->extension_count = 0;
        return;
    }
    block_relist(b, t);
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
    PT_UPDATE,    /* delta "update" track (MSF-02): parentName required */
} parse_track_mode_t;

/* Whether a track's extension list holds nothing but a supplement whose
 * only content is malformed bits: what a remove track may carry, for apply
 * to judge (MSF-02 5.2.2). */
static bool ext_only_malformed_bits(const moq_msf_track_t *t)
{
    if (t->extension_count != 1 || !t->extensions ||
        t->extensions[0].kind != MOQ_MSF_EXT_SUPPLEMENT)
        return false;
    const moq_msf_track_supplement_t *s = t->extensions[0].u.supplement;
    if (!s || s->struct_size < MOQ_MSF_TRACK_SUPPLEMENT_V0_SIZE || !s->malformed)
        return false;
    moq_msf_track_supplement_t rest = *s;
    rest.malformed = 0;
    return moq_msf_supplement_is_empty(&rest);
}

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
           !t->has_template && !t->has_track_duration &&
           (t->extension_count == 0 || ext_only_malformed_bits(t));
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

/* MSF-02 11.2.1: a naming string (a name, a namespace, a "depends" String)
 * maps to bytes as UTF-8 of Unicode scalar values; authoring also never
 * writes NUL. */
static bool naming_bytes_ok(moq_bytes_t b, bool authoring)
{
    if (b.len > 0 && !b.data) return false;
    if (!moq_msf_utf8_valid(b.data, b.len)) return false;
    return !authoring || b.len == 0 || !memchr(b.data, 0, b.len);
}

static bool naming_ns_ok(moq_bytes_t ns, bool authoring)
{
    return moq_msf_namespace_syntax_ok(ns) && naming_bytes_ok(ns, authoring);
}

/* The MSF-02 naming rules of one track (5.2.2, 5.2.3, 5.2.15, 5.2.34,
 * 5.2.35, 11.2): non-empty names, namespaces of 1..32 non-empty fields, and
 * naming strings that map to bytes. An update op track may omit its name. */
static bool track_naming_ok_r02(const moq_msf_track_t *t, bool name_required,
                                bool authoring)
{
    if (name_required && t->name.len == 0) return false;
    if (!naming_bytes_ok(t->name, authoring)) return false;
    if (t->has_namespace && !naming_ns_ok(t->namespace_, authoring)) return false;
    if (t->has_parent_name &&
        (t->parent_name.len == 0 || !naming_bytes_ok(t->parent_name, authoring)))
        return false;
    if (t->has_parent_namespace && !naming_ns_ok(t->parent_namespace, authoring))
        return false;
    if (t->depends_count > 0 && !t->depends) return false;
    for (size_t i = 0; i < t->depends_count; i++)
        if (!naming_bytes_ok(t->depends[i], authoring)) return false;
    return true;
}

/* What a parse may read: the caller's extent decides the vocabulary. */
typedef struct parse_ctx {
    const moq_alloc_t *alloc;
    bool               full;      /* the full extent: MSF-02 vocabulary */
    moq_msf_revision_t rev;       /* the independent catalog's revision */
    bool               delta;     /* parsing delta op tracks */
    bool               r02_vocab; /* an op track used MSF-02 vocabulary */
} parse_ctx_t;

/* What a parse reads into extension records. MSF-02-only track keys are
 * read for an MSF-02 catalog and for delta op tracks, which apply judges
 * against the base's revision. */
static unsigned parse_caps(const parse_ctx_t *pc)
{
    unsigned caps = 0;
    if (pc->full) {
        caps |= MSF_CAP_FULL;
        if (pc->delta || pc->rev == MOQ_MSF_REVISION_02) caps |= MSF_CAP_R02;
    }
    return caps;
}

/* A JSON number holding an integer in int's range, in plain integer form. */
static bool num_value_int(const json_value_t *v, int *out)
{
    json_number_t *n = json_value_as_number((json_value_t *)v);
    if (!n || n->number_size == 0) return false;
    char *endp;
    errno = 0;
    long x = strtol(n->number, &endp, 10);
    if (errno == ERANGE || endp != n->number + n->number_size) return false;
    if (x < INT_MIN || x > INT_MAX) return false;
    *out = (int)x;
    return true;
}

/* "depends" (MSF 5.2.14 / MSF-02 5.2.15): String entries are track names, in
 * moq_msf_track_t.depends; Number entries are renderGroup ids, in the
 * supplement, and only MSF-02 vocabulary has them. On a clone or update op
 * track the key's presence is recorded (has_depends): the op replaces both
 * lists. Anything else in the array is malformed. */
static moq_result_t parse_depends(parse_ctx_t *pc, const json_value_t *val,
                                  moq_msf_track_t *t, parse_track_mode_t mode)
{
    const moq_alloc_t *alloc = pc->alloc;
    bool numbers_ok = (parse_caps(pc) & MSF_CAP_R02) != 0;
    json_array_t *a = json_value_as_array((json_value_t *)val);
    if (!a) return MOQ_ERR_PROTO;
    size_t ns = 0, nn = 0;
    int dummy;
    for (json_array_element_t *e = a->start; e; e = e->next) {
        if (json_value_as_string(e->value)) ns++;
        else if (numbers_ok && num_value_int(e->value, &dummy)) nn++;
        else return MOQ_ERR_PROTO;
    }
    size_t sbytes = 0, nbytes = 0;
    if (ns > 0) {
        if (!moq_msf_checked_array_bytes(ns, sizeof(moq_bytes_t), &sbytes))
            return MOQ_ERR_NOMEM;
        t->depends = (moq_bytes_t *)alloc->alloc(sbytes, alloc->ctx);
        if (!t->depends) return MOQ_ERR_NOMEM;
    }
    t->depends_count = ns;     /* freed with the track on any failure below */
    bool op_presence = pc->full && pc->delta &&
                       (mode == PT_CLONE || mode == PT_UPDATE);
    int *groups = NULL;
    if (nn > 0) {
        if (!moq_msf_checked_array_bytes(nn, sizeof(int), &nbytes))
            return MOQ_ERR_NOMEM;
        groups = (int *)alloc->alloc(nbytes, alloc->ctx);
        if (!groups) return MOQ_ERR_NOMEM;
    }
    size_t si = 0, gi = 0;
    for (json_array_element_t *e = a->start; e; e = e->next) {
        json_string_t *str = json_value_as_string(e->value);
        if (str) t->depends[si++] = bytes_from_json_string(str);
        else (void)num_value_int(e->value, &groups[gi++]);
    }
    if (nn > 0 || op_presence) {
        moq_msf_track_supplement_t *sp = (moq_msf_track_supplement_t *)
            parse_ext_rec(alloc, t, MSF_ROW_SUPPLEMENT);
        if (!sp) {
            if (groups) alloc->free(groups, nbytes, alloc->ctx);
            return MOQ_ERR_NOMEM;
        }
        sp->depends_render_groups = groups;
        sp->depends_render_group_count = nn;
        sp->has_depends = op_presence;
    }
    return MOQ_OK;
}

static moq_result_t parse_track(parse_ctx_t *pc,
                                 const json_value_t *val,
                                 moq_msf_track_t *t,
                                 parse_track_mode_t mode)
{
    const moq_alloc_t *alloc = pc->alloc;
    const unsigned caps = parse_caps(pc);
    memset(t, 0, sizeof(*t));
    t->struct_size = sizeof(*t);

    json_object_t *obj = json_value_as_object((json_value_t *)val);
    if (!obj) return MOQ_ERR_PROTO;

    bool got_name = false, got_packaging = false, got_is_live = false;
    bool seen_name = false;
    bool seen_cp_refs = false;
    bool seen_depends = false;
    bool seen_template = false;

    for (json_object_element_t *e = obj->start; e; e = e->next) {
        if (str_eq(e->name, "name")) {
            seen_name = true;
            got_name = get_string(e, &t->name);
        } else if (str_eq(e->name, "packaging")) {
            got_packaging = get_string(e, &t->packaging);
        } else if (str_eq(e->name, "isLive")) {
            got_is_live = get_bool(e, &t->is_live);
        } else if (str_eq(e->name, "namespace")) {
            t->has_namespace = get_string(e, &t->namespace_);
            if (!t->has_namespace) {
                /* MSF-02 5.2.2: a namespace that is not a String makes an
                 * MSF-02 catalog invalid; on a delta op track it is recorded
                 * for apply to judge against the base's revision. MSF-01
                 * treats it as absent. */
                if (pc->rev == MOQ_MSF_REVISION_02 && !pc->delta)
                    return MOQ_ERR_PROTO;
                if (pc->full && pc->delta) {
                    moq_msf_track_supplement_t *sp = (moq_msf_track_supplement_t *)
                        parse_ext_rec(alloc, t, MSF_ROW_SUPPLEMENT);
                    if (!sp) return MOQ_ERR_NOMEM;
                    sp->malformed |= MOQ_MSF_SUPPLEMENT_F_NAMESPACE;
                }
            }
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
            moq_result_t rc = parse_depends(pc, e->value, t, mode);
            if (rc < 0) return rc;   /* present but malformed */
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
            if (!t->has_parent_namespace && pc->full && pc->delta) {
                moq_msf_track_supplement_t *sp = (moq_msf_track_supplement_t *)
                    parse_ext_rec(alloc, t, MSF_ROW_SUPPLEMENT);
                if (!sp) return MOQ_ERR_NOMEM;
                sp->malformed |= MOQ_MSF_SUPPLEMENT_F_PARENT_NAMESPACE;
            }
        } else {
            /* A key an extension kind owns (the mpeg2ts* keys of
             * draft-gregoire-moq-msfts, the supplement's MSF keys); any other
             * key is not understood and ignored. */
            for (size_t r = 0; r < moq_msf_ext_kind_count; r++) {
                const msf_ext_kind_ops_t *k = &moq_msf_ext_kinds[r];
                if (!k->key_known(e->name, caps)) continue;
                void *rec = parse_ext_rec(alloc, t, r);
                if (!rec) return MOQ_ERR_NOMEM;   /* caller frees the track */
                moq_result_t rc = k->parse_key(alloc, e, rec);
                if (rc < 0) return rc;
                break;
            }
        }
    }
    parse_ext_finish(alloc, t);
    /* A delta's revision follows the vocabulary its op tracks use. */
    if (pc->delta && moq_msf_supplement_has_r02_fields(moq_msf_supplement_of(t)))
        pc->r02_vocab = true;

    /* Record presence of the otherwise-required keys for clone-override
     * fidelity (the encoder still emits them unconditionally for full/add). */
    t->has_packaging = got_packaging;
    t->has_is_live = got_is_live;

    /* MSF 5.2.34/5.2.35: parentName/parentNamespace appear only in a clone
     * or (MSF-02) an update op. */
    if (mode != PT_CLONE && mode != PT_UPDATE &&
        (t->has_parent_name || t->has_parent_namespace))
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
    case PT_UPDATE:
        /* MSF-02 5.1.6 update: parentName names the track; a name, when the
         * object carries one, is a non-empty String (apply holds it to the
         * target's: an update never renames). */
        if (!t->has_parent_name || (seen_name && (!got_name || t->name.len == 0)))
            return MOQ_ERR_PROTO;
        break;
    }
    /* An MSF-02 catalog's own tracks follow MSF-02's naming rules; a delta's
     * op tracks are judged by apply, against the base's revision. */
    if (!pc->delta && pc->rev == MOQ_MSF_REVISION_02 &&
        !track_naming_ok_r02(t, true, false))
        return MOQ_ERR_PROTO;

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

/* Parse a JSON array of track objects into a catalog-owned array (NULL/0
 * when empty). On failure nothing stays allocated. */
static moq_result_t parse_track_array(parse_ctx_t *pc, const json_array_t *arr,
                                      parse_track_mode_t mode,
                                      moq_msf_track_t **out, size_t *out_count)
{
    const moq_alloc_t *alloc = pc->alloc;
    *out = NULL;
    *out_count = 0;
    size_t n = arr->length;
    if (n == 0) return MOQ_OK;
    size_t tbytes;
    if (!moq_msf_checked_array_bytes(n, sizeof(moq_msf_track_t), &tbytes))
        return MOQ_ERR_NOMEM;
    moq_msf_track_t *tracks = (moq_msf_track_t *)alloc->alloc(tbytes, alloc->ctx);
    if (!tracks) return MOQ_ERR_NOMEM;
    size_t parsed = 0;
    const json_array_element_t *ae = arr->start;
    for (size_t i = 0; i < n && ae; i++, ae = ae->next) {
        moq_result_t rc = parse_track(pc, ae->value, &tracks[parsed], mode);
        if (rc < 0) {
            free_track_allocs(alloc, &tracks[parsed]);   /* the failed track */
            for (size_t j = 0; j < parsed; j++) free_track_allocs(alloc, &tracks[j]);
            alloc->free(tracks, tbytes, alloc->ctx);
            return rc;
        }
        parsed++;
    }
    *out = tracks;
    *out_count = parsed;
    return MOQ_OK;
}

/* Parse one deltaUpdate operation object (MSF 5.1.6): {op, tracks:[...]}. */
static moq_result_t parse_delta_op(parse_ctx_t *pc,
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
    else if (pc->full && bytes_eq_lit(op_str, "update")) {
        /* MSF-02 5.1.6, read only by a caller whose struct holds MSF-02. */
        out->op = MOQ_MSF_DELTA_OP_UPDATE;
        mode = PT_UPDATE;
        pc->r02_vocab = true;
    }
    else return MOQ_ERR_PROTO;                /* unknown op type */

    if (tracks_arr->length == 0) return MOQ_ERR_PROTO;  /* >= 1 track */
    return parse_track_array(pc, tracks_arr, mode, &out->tracks,
                             &out->track_count);
}

/* -- Public API ------------------------------------------------------ */

static void free_track_array(const moq_alloc_t *alloc, moq_msf_track_t *tracks,
                             size_t count)
{
    if (!tracks) return;
    for (size_t i = 0; i < count; i++) free_track_allocs(alloc, &tracks[i]);
    alloc->free(tracks, count * sizeof(moq_msf_track_t), alloc->ctx);
}

/* Parse a document into a catalog of the full layout. `full` selects the
 * MSF-02 vocabulary (the caller's extent holds the MSF-02 block). On error
 * everything is released and *out is zeroed. */
static moq_result_t parse_doc(const moq_alloc_t *alloc, moq_bytes_t json,
                              bool full, moq_msf_catalog_t *out)
{
    memset(out, 0, sizeof(*out));
    out->struct_size = sizeof(*out);

    alloc_ctx_t actx = { .alloc = alloc, .total = 0, .base = NULL };
    json_parse_result_t err;
    json_value_t *root = json_parse_ex(
        json.data, json.len, json_parse_flags_default,
        json_alloc_bridge, &actx, &err);
    if (!root) {
        if (actx.base) alloc->free(actx.base, actx.total, alloc->ctx);
        if (err.error == json_parse_error_allocator_failed)
            return MOQ_ERR_NOMEM;
        return MOQ_ERR_PROTO;
    }
    out->_dom = actx.base;
    out->_dom_size = actx.total;
    msf_dom_hdr_t *hdr = (msf_dom_hdr_t *)(void *)actx.base;
    hdr->magic = MSF_DOM_MAGIC;

    moq_result_t rc = MOQ_ERR_PROTO;
    parse_ctx_t pc = { .alloc = alloc, .full = full,
                       .rev = MOQ_MSF_REVISION_01, .delta = false,
                       .r02_vocab = false };
    json_object_t *top = json_value_as_object(root);
    if (!top) goto fail;

    bool got_version = false;
    bool seen_tracks = false;
    bool seen_is_complete = false;
    json_array_t *tracks_arr = NULL;
    json_array_t *publish_arr = NULL;
    json_array_t *idl_arr = NULL;
    json_array_t *cp_arr = NULL;
    json_array_t *delta_arr = NULL;

    for (json_object_element_t *e = top->start; e; e = e->next) {
        if (str_eq(e->name, "version")) {
            hdr->root_keys |= MSF_ROOT_VERSION;
            if (!version_is_supported(e, full, &out->version_form, &pc.rev))
                goto fail;
            out->version = MOQ_MSF_VERSION;
            got_version = true;
        } else if (str_eq(e->name, "tracks")) {
            hdr->root_keys |= MSF_ROOT_TRACKS;
            seen_tracks = true;
            tracks_arr = json_value_as_array(e->value);
        } else if (str_eq(e->name, "generatedAt")) {
            hdr->root_keys |= MSF_ROOT_GENERATED_AT;
            out->has_generated_at = get_u64(e, &out->generated_at);
        } else if (str_eq(e->name, "isComplete")) {
            /* MSF 5.1.3: only the TRUE state is meaningful; a false/malformed
             * value is treated as absent (the spec forbids emitting false). The
             * key's presence is still recorded so a delta carrying it is
             * rejected below. */
            hdr->root_keys |= MSF_ROOT_IS_COMPLETE;
            seen_is_complete = true;
            bool b = false;
            if (get_bool(e, &b) && b) out->is_complete = true;
        } else if (str_eq(e->name, "publishTracks")) {
            /* MSF 5.1.5, read only by a caller whose struct holds it. */
            hdr->root_keys |= MSF_ROOT_PUBLISH_TRACKS;
            if (full) {
                publish_arr = json_value_as_array(e->value);
                if (!publish_arr) goto fail;   /* present but not an array */
            }
        } else if (str_eq(e->name, "initDataList")) {
            hdr->root_keys |= MSF_ROOT_INIT_DATA_LIST;
            idl_arr = json_value_as_array(e->value);
            if (!idl_arr) goto fail;           /* present but not an array */
        } else if (str_eq(e->name, "contentProtections")) {
            hdr->root_keys |= MSF_ROOT_CONTENT_PROTECTIONS;
            cp_arr = json_value_as_array(e->value);
            if (!cp_arr) goto fail;            /* present but not an array */
        } else if (str_eq(e->name, "deltaUpdate")) {
            hdr->root_keys |= MSF_ROOT_DELTA_UPDATE;
            delta_arr = json_value_as_array(e->value);
            if (!delta_arr) goto fail;         /* present but not an array */
        }
    }

    /* MSF 5.1.6/5.3: deltaUpdate marks a partial update. A delta MUST NOT carry
     * a version, tracks, or isComplete field and MUST hold at least one
     * operation (5.1.3: completion is signalled by an independent catalog). */
    if (delta_arr) {
        if (got_version || seen_tracks || seen_is_complete ||
            delta_arr->length == 0)
            goto fail;
        size_t oc = delta_arr->length;
        size_t ob;
        if (!moq_msf_checked_array_bytes(oc, sizeof(moq_msf_delta_op_t), &ob)) {
            rc = MOQ_ERR_NOMEM;
            goto fail;
        }
        moq_msf_delta_op_t *ops = (moq_msf_delta_op_t *)alloc->alloc(
            ob, alloc->ctx);
        if (!ops) { rc = MOQ_ERR_NOMEM; goto fail; }
        pc.delta = true;
        size_t poc = 0;
        json_array_element_t *oe = delta_arr->start;
        for (size_t i = 0; i < oc && oe; i++, oe = oe->next) {
            rc = parse_delta_op(&pc, oe->value, &ops[poc]);
            if (rc < 0) {
                for (size_t j = 0; j < poc; j++) free_delta_op_allocs(alloc, &ops[j]);
                alloc->free(ops, ob, alloc->ctx);
                goto fail;
            }
            poc++;
        }
        out->delta_update = ops;
        out->delta_update_count = poc;
        /* A delta has no version: report the earliest revision whose
         * vocabulary covers it. */
        out->revision = pc.r02_vocab ? MOQ_MSF_REVISION_02 : MOQ_MSF_REVISION_01;
        return MOQ_OK;   /* a delta carries no tracks/idl/cp of its own */
    }

    if (!got_version || !tracks_arr) goto fail;
    out->revision = pc.rev;

    rc = parse_track_array(&pc, tracks_arr, PT_FULL, &out->tracks,
                           &out->track_count);
    if (rc < 0) goto fail;
    if (publish_arr) {
        rc = parse_track_array(&pc, publish_arr, PT_FULL, &out->publish_tracks,
                               &out->publish_track_count);
        if (rc < 0) goto fail;
    }

    /* Initialization Data List (MSF 5.1.7) -- present after tracks in the
     * document; parse order here is independent of that. */
    if (idl_arr && idl_arr->length > 0) {
        size_t ic = idl_arr->length;
        size_t ib;
        if (!moq_msf_checked_array_bytes(ic, sizeof(moq_msf_init_data_entry_t),
                                         &ib)) {
            rc = MOQ_ERR_NOMEM;
            goto fail;
        }
        moq_msf_init_data_entry_t *entries =
            (moq_msf_init_data_entry_t *)alloc->alloc(ib, alloc->ctx);
        if (!entries) { rc = MOQ_ERR_NOMEM; goto fail; }

        size_t pic = 0;
        json_array_element_t *ie = idl_arr->start;
        for (size_t i = 0; i < ic && ie; i++, ie = ie->next) {
            rc = parse_init_data_entry(ie->value, &entries[pic]);
            if (rc < 0) {
                alloc->free(entries, ib, alloc->ctx);
                goto fail;
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
            rc = MOQ_ERR_NOMEM;
            goto fail;
        }
        moq_cmsf_content_protection_t *cps =
            (moq_cmsf_content_protection_t *)alloc->alloc(cb, alloc->ctx);
        if (!cps) { rc = MOQ_ERR_NOMEM; goto fail; }

        size_t pcc = 0;
        json_array_element_t *ce = cp_arr->start;
        for (size_t i = 0; i < cc && ce; i++, ce = ce->next) {
            rc = parse_content_protection(alloc, ce->value, &cps[pcc]);
            if (rc < 0) {
                for (size_t j = 0; j < pcc; j++) free_cp_allocs(alloc, &cps[j]);
                alloc->free(cps, cb, alloc->ctx);
                goto fail;
            }
            pcc++;
        }

        out->content_protections = cps;
        out->content_protection_count = pcc;
    }

    return MOQ_OK;

fail:
    moq_msf_catalog_cleanup(alloc, out);
    memset(out, 0, sizeof(*out));
    return rc;
}

/* The sized parse: refuse an extent it cannot honour before writing; then
 * write min(out_size, sizeof) bytes of the result (zeroed on error) and stamp
 * that count. */
static moq_result_t parse_impl(const moq_alloc_t *alloc, moq_bytes_t json,
                               moq_msf_catalog_t *out, size_t out_size)
{
    if (!alloc || !json.data || !out) return MOQ_ERR_INVAL;
    msf_extent_t x = moq_msf_extent_of(out_size);
    if (x == MSF_EXTENT_SHORT || x == MSF_EXTENT_PARTIAL) return MOQ_ERR_INVAL;
    size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);
    moq_msf_catalog_t cat;
    moq_result_t rc = parse_doc(alloc, json, x == MSF_EXTENT_FULL, &cat);
    cat.struct_size = (uint32_t)n;
    memcpy(out, &cat, n);
    return rc;
}

moq_result_t moq_msf_catalog_parse_sized(const moq_alloc_t *alloc,
                                         moq_bytes_t json,
                                         moq_msf_catalog_t *out,
                                         size_t out_size)
{
    return parse_impl(alloc, json, out, out_size);
}

/* Parenthesized: defines the exported MSF-01 symbol, not the header's
 * sized macro. */
moq_result_t (moq_msf_catalog_parse)(const moq_alloc_t *alloc,
                                      moq_bytes_t json,
                                      moq_msf_catalog_t *out)
{
    return parse_impl(alloc, json, out,
                      MOQ_MSF_CATALOG_SIZE_THROUGH_VERSION_FORM);
}

void moq_msf_catalog_cleanup(const moq_alloc_t *alloc,
                              moq_msf_catalog_t *cat)
{
    if (!alloc || !cat) return;
    if (moq_msf_extent_of(cat->struct_size) == MSF_EXTENT_FULL) {
        free_track_array(alloc, cat->publish_tracks, cat->publish_track_count);
        cat->publish_tracks = NULL;
        cat->publish_track_count = 0;
    }
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

static bool bytes_eq(moq_bytes_t a, moq_bytes_t b);   /* defined below */

/* An identity span is safe to hash/compare only if it is not the reserved
 * byte-span empty-slot sentinel (len == SIZE_MAX) and does not claim content
 * with a NULL data pointer. Parsed catalogs never violate this; public callers
 * that hand-build a catalog can, so identity spans are checked before any
 * hash/memcmp reaches the identity indexes. */
static bool ident_span_ok(moq_bytes_t b)
{
    if (b.len == SIZE_MAX) return false;      /* the indexes' empty-slot sentinel */
    if (b.len > 0 && !b.data) return false;   /* content with no buffer */
    return true;
}

/* Validate the identity spans of one track: its name, and its namespace when
 * declared. (Other metadata is not an identity span and is not checked here --
 * receive-path leniency for non-identity fields is preserved.) */
static bool track_identity_ok(const moq_msf_track_t *t)
{
    if (!ident_span_ok(t->name)) return false;
    if (t->has_namespace && !ident_span_ok(t->namespace_)) return false;
    return true;
}

/* Build an effective track from a clone parent plus the clone op's overrides.
 * eff begins as a copy of the parent; spans are borrowed (no allocation). */
/* mpeg2ts fields of a clone: each key the clone carries overrides the
 * inherited one, value and malformed bit together. Spans and arrays stay
 * borrowed from the catalogs they came from. */
static void ts_merge_clone(moq_msf_mpeg2ts_t *em, const moq_msf_mpeg2ts_t *cm)
{
#define MPEG2TS_OVERRIDE(has, bit, assign) \
    if (cm->has) { em->has = true; assign; \
                   em->malformed = (em->malformed & ~(bit)) | (cm->malformed & (bit)); }
    MPEG2TS_OVERRIDE(has_mode, MOQ_MSF_MPEG2TS_F_MODE,
                     (em->mode = cm->mode, em->mode_name = cm->mode_name))
    MPEG2TS_OVERRIDE(has_packet_size, MOQ_MSF_MPEG2TS_F_PACKET_SIZE,
                     em->packet_size = cm->packet_size)
    MPEG2TS_OVERRIDE(has_es_pid, MOQ_MSF_MPEG2TS_F_ES_PID, em->es_pid = cm->es_pid)
    MPEG2TS_OVERRIDE(has_program_number, MOQ_MSF_MPEG2TS_F_PROGRAM_NUMBER,
                     em->program_number = cm->program_number)
    MPEG2TS_OVERRIDE(has_pcr_pid, MOQ_MSF_MPEG2TS_F_PCR_PID, em->pcr_pid = cm->pcr_pid)
    MPEG2TS_OVERRIDE(has_mux_rate, MOQ_MSF_MPEG2TS_F_MUX_RATE,
                     em->mux_rate = cm->mux_rate)
    MPEG2TS_OVERRIDE(has_si_pids, MOQ_MSF_MPEG2TS_F_SI_PIDS,
                     (em->si_pids = cm->si_pids, em->si_pid_count = cm->si_pid_count))
    MPEG2TS_OVERRIDE(has_random_access, MOQ_MSF_MPEG2TS_F_RANDOM_ACCESS,
                     em->random_access = cm->random_access)
    MPEG2TS_OVERRIDE(has_timestamp_mode, MOQ_MSF_MPEG2TS_F_TIMESTAMP_MODE,
                     (em->timestamp_mode = cm->timestamp_mode,
                      em->timestamp_mode_name = cm->timestamp_mode_name))
    MPEG2TS_OVERRIDE(has_scte35_pid, MOQ_MSF_MPEG2TS_F_SCTE35_PID,
                     em->scte35_pid = cm->scte35_pid)
#undef MPEG2TS_OVERRIDE
}

/* The fields an op track carries override the inherited or existing ones
 * (clone, update). The namespace is the clone's to set: an update names its
 * target and never moves it. */
static void apply_field_overrides(moq_msf_track_t *eff,
                                  const moq_msf_track_t *c,
                                  moq_msf_revision_t rev, bool namespace_too)
{
    /* Parent markers do not belong on an effective independent track. */
    eff->has_parent_name = false;
    eff->has_parent_namespace = false;
    eff->parent_name = (moq_bytes_t){ NULL, 0 };
    eff->parent_namespace = (moq_bytes_t){ NULL, 0 };

    if (c->has_packaging) eff->packaging = c->packaging;
    if (c->has_is_live)   eff->is_live = c->is_live;
    eff->has_packaging = true;           /* an effective full track has both */
    eff->has_is_live = true;

    if (namespace_too && c->has_namespace) {
        eff->has_namespace = true; eff->namespace_ = c->namespace_;
    }
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
    /* MSF-02: an op that carried "depends" replaces the list, even with an
     * empty one; MSF-01 replaces it only with entries. */
    const moq_msf_track_supplement_t *cs =
        rev == MOQ_MSF_REVISION_02 ? moq_msf_supplement_of(c) : NULL;
    if ((cs && cs->has_depends) || c->depends_count > 0) {
        eff->depends = c->depends; eff->depends_count = c->depends_count;
    }
    if (c->cp_ref_id_count > 0){ eff->cp_ref_ids = c->cp_ref_ids; eff->cp_ref_id_count = c->cp_ref_id_count; }
    /* eff began as a copy of the parent, so it already inherits the parent's
     * template; a clone carrying its own template overrides it (5.2.15). */
    if (c->has_template)       { eff->has_template = true; eff->template_ = c->template_; }
}

static void apply_clone_overrides(moq_msf_track_t *eff,
                                  const moq_msf_track_t *c,
                                  moq_msf_revision_t rev)
{
    eff->name = c->name;                 /* the new track name is mandatory */
    apply_field_overrides(eff, c, rev, true);
}

static bool template_eq(const moq_msf_media_template_t *a,
                        const moq_msf_media_template_t *b)
{
    return a->start_media_ms == b->start_media_ms &&
           a->delta_media_ms == b->delta_media_ms &&
           a->start_group == b->start_group &&
           a->start_object == b->start_object &&
           a->delta_group == b->delta_group &&
           a->delta_object == b->delta_object &&
           a->start_wallclock_ms == b->start_wallclock_ms &&
           a->delta_wallclock_ms == b->delta_wallclock_ms;
}

/* MSF-02 "update" (5.1.6) on its target: a name or namespace the op carries
 * is the target's own, so an update never renames; isLive never turns from
 * false to true (5.2.8); a template the target has never changes (7.4.2),
 * though one may be added. */
static bool update_allowed(const msf_ident_mode_t *mode,
                           const moq_msf_track_t *target,
                           const moq_msf_track_t *u)
{
    if (u->name.len > 0 && !bytes_eq(u->name, target->name)) return false;
    if (u->has_namespace &&
        !moq_msf_ident_eq(mode, true, u->namespace_, target->name,
                          target->has_namespace, target->namespace_, target->name))
        return false;
    if (u->has_is_live && u->is_live && !target->is_live) return false;
    if (u->has_template && target->has_template &&
        !template_eq(&u->template_, &target->template_))
        return false;
    return true;
}

/* One temporary allocation, with its size for the free. */
typedef struct msf_tmp_block {
    void  *p;
    size_t n;
} msf_tmp_block_t;

/* The alignment of every record kind, for packing records after a list. */
#define MSF_REC_ALIGN 8u
_Static_assert(MSF_REC_ALIGN % _Alignof(moq_msf_mpeg2ts_t) == 0 &&
               MSF_REC_ALIGN % _Alignof(moq_msf_track_supplement_t) == 0 &&
               MSF_REC_ALIGN % _Alignof(moq_msf_track_ext_t) == 0,
               "records packed after a list stay aligned");

static bool align_up(size_t n, size_t *out)
{
    if (n > SIZE_MAX - (MSF_REC_ALIGN - 1)) return false;
    *out = (n + MSF_REC_ALIGN - 1) / MSF_REC_ALIGN * MSF_REC_ALIGN;
    return true;
}

/* A clone's or update's extension list: the target's entries, with each
 * kind the op track carries folded, field by field, into a fresh record
 * (the kind's merge), or appended. The merged list and records live in one
 * allocation the caller keeps until the effective catalog is encoded
 * (*block, *block_size). Spans and arrays stay borrowed from the catalogs
 * they came from. */
static moq_result_t clone_merge_ext(const moq_alloc_t *alloc,
                                    moq_msf_track_t *eff,
                                    const moq_msf_track_t *c,
                                    moq_msf_revision_t rev,
                                    void **block, size_t *block_size)
{
    *block = NULL;
    *block_size = 0;
    size_t pn = eff->extension_count, cn = c->extension_count;
    if (cn == 0) return MOQ_OK;
    if (!c->extensions || (pn && !eff->extensions)) return MOQ_ERR_INVAL;
    for (size_t i = 0; i < cn; i++)
        for (size_t j = i + 1; j < cn; j++)
            if (c->extensions[i].kind == c->extensions[j].kind)
                return MOQ_ERR_INVAL;
    if (pn > SIZE_MAX / 2 || cn > SIZE_MAX / 2) return MOQ_ERR_NOMEM;
    size_t list_bytes, off;
    if (!moq_msf_checked_array_bytes(pn + cn, sizeof(moq_msf_track_ext_t),
                                     &list_bytes) ||
        !align_up(list_bytes, &off))
        return MOQ_ERR_NOMEM;
    size_t bytes = off;
    for (size_t j = 0; j < cn; j++) {
        const msf_ext_kind_ops_t *k = moq_msf_ext_kind_ops(c->extensions[j].kind);
        size_t rs;
        if (!k) return MOQ_ERR_INVAL;
        if (!align_up(k->record_size, &rs) || rs > SIZE_MAX - bytes)
            return MOQ_ERR_NOMEM;
        bytes += rs;
    }
    uint8_t *mem = (uint8_t *)alloc->alloc(bytes, alloc->ctx);
    if (!mem) return MOQ_ERR_NOMEM;
    moq_msf_track_ext_t *list = (moq_msf_track_ext_t *)(void *)mem;
    size_t k_n = 0;
    for (size_t i = 0; i < pn; i++) list[k_n++] = eff->extensions[i];
    for (size_t j = 0; j < cn; j++) {
        const moq_msf_track_ext_t *ce = &c->extensions[j];
        const msf_ext_kind_ops_t *k = moq_msf_ext_kind_ops(ce->kind);
        void *rec = mem + off;
        size_t rs;
        (void)align_up(k->record_size, &rs);
        off += rs;
        size_t at = k_n;
        for (size_t i = 0; i < k_n; i++)
            if (list[i].kind == ce->kind) { at = i; break; }
        k->init(rec);
        const void *inherited = at < k_n ? moq_msf_ext_record(&list[at]) : NULL;
        if (inherited) {
            uint32_t isz;
            memcpy(&isz, inherited, sizeof(isz));
            if (isz >= k->v0_size) {
                memcpy(rec, inherited, isz < k->record_size ? isz : k->record_size);
                uint32_t stamp = (uint32_t)k->record_size;
                memcpy(rec, &stamp, sizeof(stamp));
            }
        }
        k->merge(rec, moq_msf_ext_record(ce), rev);
        ext_entry_set(&list[at], ce->kind, rec);
        if (at == k_n) k_n++;
    }
    eff->extensions = list;
    eff->extension_count = k_n;
    *block = mem;
    *block_size = bytes;
    return MOQ_OK;
}

/* Internal encode: strict_cp=true enforces CMSF §4 contentProtection authoring
 * syntax; false keeps it lenient for the receive-path re-encode below. */
static moq_result_t catalog_encode_impl(const moq_alloc_t *alloc,
                                        const moq_msf_catalog_t *cat,
                                        moq_rcbuf_t **out_json, bool strict_cp);

/* -- Caller storage ------------------------------------------------------
 * What a public caller building a catalog by hand can get wrong and a parse
 * never produces: a span that claims bytes with no buffer, an array that
 * claims entries with none, an extension entry without a usable record.
 * apply refuses these as MOQ_ERR_INVAL before it judges anything, so that
 * whatever the effective catalog then fails is the peer's (MOQ_ERR_PROTO). */

static bool span_ok(moq_bytes_t b) { return b.len == 0 || b.data != NULL; }

static bool spans_ok(const moq_bytes_t *v, size_t n)
{
    if (n > 0 && !v) return false;
    for (size_t i = 0; i < n; i++)
        if (!span_ok(v[i])) return false;
    return true;
}

/* The extension list: a known kind at most once, each with a record of at
 * least its first published size. */
static bool ext_list_ok(const moq_msf_track_t *t)
{
    if (t->extension_count > 0 && !t->extensions) return false;
    for (size_t i = 0; i < t->extension_count; i++) {
        const moq_msf_track_ext_t *e = &t->extensions[i];
        const msf_ext_kind_ops_t *k = moq_msf_ext_kind_ops(e->kind);
        const void *rec = moq_msf_ext_record(e);
        if (!k || !rec) return false;
        uint32_t size;
        memcpy(&size, rec, sizeof(size));
        if (size < k->v0_size || !k->storage_ok(rec)) return false;
        for (size_t j = i + 1; j < t->extension_count; j++)
            if (t->extensions[j].kind == e->kind) return false;
    }
    return true;
}

static bool track_storage_ok(const moq_msf_track_t *t)
{
    if (!span_ok(t->name) || !span_ok(t->packaging)) return false;
#define OPT_SPAN(has, f) if (t->has && !span_ok(t->f)) return false
    OPT_SPAN(has_namespace, namespace_);
    OPT_SPAN(has_role, role);
    OPT_SPAN(has_codec, codec);
    OPT_SPAN(has_init_data, init_data);
    OPT_SPAN(has_init_track, init_track);
    OPT_SPAN(has_channel_config, channel_config);
    OPT_SPAN(has_lang, lang);
    OPT_SPAN(has_label, label);
    OPT_SPAN(has_init_ref, init_ref);
    OPT_SPAN(has_event_type, event_type);
    OPT_SPAN(has_mime_type, mime_type);
    OPT_SPAN(has_parent_name, parent_name);
    OPT_SPAN(has_parent_namespace, parent_namespace);
#undef OPT_SPAN
    if (!spans_ok(t->depends, t->depends_count)) return false;
    if (!spans_ok(t->cp_ref_ids, t->cp_ref_id_count)) return false;
    return ext_list_ok(t);
}

static bool tracks_storage_ok(const moq_msf_track_t *t, size_t n)
{
    if (n > 0 && !t) return false;
    for (size_t i = 0; i < n; i++)
        if (!track_storage_ok(&t[i])) return false;
    return true;
}

static bool url_storage_ok(const moq_cmsf_url_t *u)
{
    return !u->present || (span_ok(u->url) && (!u->has_type || span_ok(u->type)));
}

/* An independent base: its tracks, publish tracks, initDataList and
 * contentProtections, and the version form the effective catalog keeps. */
static bool base_storage_ok(const moq_msf_catalog_t *c)
{
    if (moq_msf_catalog_revision_of(c) == MOQ_MSF_REVISION_01 &&
        catalog_has_version_form(c) &&
        c->version_form != MOQ_MSF_VERSION_FORM_NUMERAL &&
        c->version_form != MOQ_MSF_VERSION_FORM_DRAFT_NAME)
        return false;
    if (!tracks_storage_ok(c->tracks, c->track_count)) return false;
    for (size_t i = 0; i < c->track_count; i++)
        if (!track_identity_ok(&c->tracks[i])) return false;
    const moq_msf_track_t *pt;
    size_t pn = moq_msf_publish_tracks_of(c, &pt);
    if (!tracks_storage_ok(pt, pn)) return false;
    if (c->init_data_count > 0 && !c->init_data_list) return false;
    for (size_t i = 0; i < c->init_data_count; i++) {
        const moq_msf_init_data_entry_t *e = &c->init_data_list[i];
        if (!span_ok(e->id) || !span_ok(e->type) || !span_ok(e->data))
            return false;
    }
    if (c->content_protection_count > 0 && !c->content_protections) return false;
    for (size_t i = 0; i < c->content_protection_count; i++) {
        const moq_cmsf_content_protection_t *cp = &c->content_protections[i];
        const moq_cmsf_drm_system_t *ds = &cp->drm_system;
        if (!span_ok(cp->ref_id) || !span_ok(cp->scheme) ||
            !spans_ok(cp->default_kids, cp->default_kid_count) ||
            !span_ok(ds->system_id) || !url_storage_ok(&ds->la_url) ||
            !url_storage_ok(&ds->cert_url) || !url_storage_ok(&ds->auth_url) ||
            (ds->has_pssh && !span_ok(ds->pssh)) ||
            (ds->has_robustness && !span_ok(ds->robustness)))
            return false;
    }
    return true;
}

/* -- Validate cfg ---------------------------------------------------------- */

_Static_assert(MOQ_MSF_VALIDATE_CFG_V0_SIZE == sizeof(moq_msf_validate_cfg_t),
               "the validate cfg's first published layout is the whole struct");

void moq_msf_validate_cfg_init_sized(moq_msf_validate_cfg_t *cfg,
                                     size_t cfg_size)
{
    if (!cfg || cfg_size < sizeof(uint32_t)) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    memset(cfg, 0, n);
    uint32_t stamp = (uint32_t)n;
    memcpy(cfg, &stamp, sizeof(stamp));
}

/* Read a caller's cfg through its own extent into a local of this build's
 * layout. NULL is the default. A catalog namespace must be a valid MOQT
 * namespace: up to 32 fields, each non-empty. */
moq_result_t moq_msf_read_validate_cfg(const moq_msf_validate_cfg_t *cfg,
                                       moq_msf_validate_cfg_t *local)
{
    memset(local, 0, sizeof(*local));
    local->struct_size = sizeof(*local);
    if (!cfg) return MOQ_OK;
    uint32_t size;
    memcpy(&size, cfg, sizeof(size));
    if (size < MOQ_MSF_VALIDATE_CFG_V0_SIZE) return MOQ_ERR_INVAL;
    memcpy(local, cfg, MOQ_MSF_VALIDATE_CFG_V0_SIZE);
    local->struct_size = sizeof(*local);
    const moq_namespace_t *ns = &local->catalog_namespace;
    if (ns->count > MOQ_MSF_NAMESPACE_MAX_FIELDS) return MOQ_ERR_INVAL;
    if (ns->count > 0 && !ns->parts) return MOQ_ERR_INVAL;
    for (size_t i = 0; i < ns->count; i++)
        if (ns->parts[i].len == 0 || !ns->parts[i].data) return MOQ_ERR_INVAL;
    return MOQ_OK;
}

/* -- Apply ------------------------------------------------------------------- */

typedef struct apply_in {
    size_t          lim_ops;      /* 0 = no limit */
    size_t          lim_tracks;   /* 0 = no limit */
    moq_namespace_t catalog_ns;   /* count 0 = unknown */
    bool            legacy;       /* the MSF-01 entry points */
} apply_in_t;

/* MSF-02 5.3: a delta carries generatedAt and no other root field. A parsed
 * delta is judged by what its JSON held, whatever the values; its public
 * fields, and those of a caller-built delta, must be empty too. */
static bool delta_roots_ok_r02(const moq_msf_catalog_t *delta)
{
    if (!delta->has_generated_at) return false;
    const msf_dom_hdr_t *h = moq_msf_dom_hdr(delta);
    if (h && (h->root_keys & ~(uint32_t)(MSF_ROOT_GENERATED_AT |
                                         MSF_ROOT_DELTA_UPDATE)) != 0)
        return false;
    const moq_msf_track_t *pt;
    return delta->track_count == 0 && !delta->is_complete &&
           delta->init_data_count == 0 && delta->content_protection_count == 0 &&
           moq_msf_publish_tracks_of(delta, &pt) == 0;
}

/* The work of apply; *out is written only by the final parse. */
static moq_result_t apply_run(const moq_alloc_t *alloc,
                              const moq_msf_catalog_t *base,
                              const moq_msf_catalog_t *delta,
                              const apply_in_t *in,
                              moq_msf_catalog_t *out, size_t out_size)
{
    if (moq_msf_extent_of(base->struct_size) == MSF_EXTENT_PARTIAL ||
        moq_msf_extent_of(delta->struct_size) == MSF_EXTENT_PARTIAL)
        return MOQ_ERR_INVAL;
    if (base->delta_update) return MOQ_ERR_INVAL;     /* base must be independent */
    if (!delta->delta_update) return MOQ_ERR_INVAL;   /* delta must be a delta */
    moq_msf_revision_t rev = moq_msf_catalog_revision_of(base);
    if (rev != MOQ_MSF_REVISION_01 && rev != MOQ_MSF_REVISION_02)
        return MOQ_ERR_INVAL;
    /* Never downgrade an MSF-02 catalog into the MSF-01 extent. */
    if (rev == MOQ_MSF_REVISION_02 &&
        (in->legacy || moq_msf_extent_of(out_size) != MSF_EXTENT_FULL))
        return MOQ_ERR_INVAL;
    /* MSF 5.1.3: isComplete is terminal -- a complete catalog commits that no
     * tracks or content will follow, so applying any delta to it is a protocol
     * error (and would otherwise drop the MUST-NOT-be-removed isComplete). */
    if (base->is_complete) return MOQ_ERR_PROTO;
    if (!base_storage_ok(base)) return MOQ_ERR_INVAL;

    /* Cap-before-apply: refuse an over-large delta before allocating the
     * effective catalog, building the index, or scanning any identity. */
    if (in->lim_ops && delta->delta_update_count > in->lim_ops)
        return MOQ_ERR_PROTO;

    /* Capacity = base tracks + every add/clone track (removes only shrink).
     * Guard the running sum against size_t wrap, then the byte size against
     * wrap, so a crafted base/delta cannot under-allocate eff and overflow it
     * during the copy/apply loop below. */
    size_t cap = base->track_count, clones = 0;
    for (size_t i = 0; i < delta->delta_update_count; i++) {
        const moq_msf_delta_op_t *op = &delta->delta_update[i];
        if (op->track_count > 0 && !op->tracks) return MOQ_ERR_INVAL;
        if (op->op != MOQ_MSF_DELTA_OP_ADD && op->op != MOQ_MSF_DELTA_OP_REMOVE &&
            op->op != MOQ_MSF_DELTA_OP_CLONE && op->op != MOQ_MSF_DELTA_OP_UPDATE)
            return MOQ_ERR_INVAL;
        /* clone and update merge extension lists: one block per track. */
        if (op->op == MOQ_MSF_DELTA_OP_CLONE || op->op == MOQ_MSF_DELTA_OP_UPDATE) {
            if (op->track_count > SIZE_MAX - clones) return MOQ_ERR_NOMEM;
            clones += op->track_count;
        }
        if (op->op == MOQ_MSF_DELTA_OP_ADD || op->op == MOQ_MSF_DELTA_OP_CLONE) {
            if (op->track_count > SIZE_MAX - cap)
                return MOQ_ERR_NOMEM;
            cap += op->track_count;
        }
    }
    if (in->lim_tracks && cap > in->lim_tracks)
        return MOQ_ERR_PROTO;
    size_t eff_bytes = 0, merged_bytes = 0;
    if (!moq_msf_checked_array_bytes(cap, sizeof(moq_msf_track_t), &eff_bytes) ||
        !moq_msf_checked_array_bytes(clones, sizeof(msf_tmp_block_t),
                                     &merged_bytes))
        return MOQ_ERR_NOMEM;
    /* Per-op-track storage, after the wrap guards above: a lying track_count
     * is refused as NOMEM before anything iterates it. */
    for (size_t i = 0; i < delta->delta_update_count; i++) {
        const moq_msf_delta_op_t *op = &delta->delta_update[i];
        for (size_t j = 0; j < op->track_count; j++)
            if (!track_storage_ok(&op->tracks[j])) return MOQ_ERR_INVAL;
    }

    if (rev == MOQ_MSF_REVISION_02 && !delta_roots_ok_r02(delta))
        return MOQ_ERR_PROTO;
    /* MSF-01 has no "update" op and no Number "depends" entries (MSF-02
     * 5.1.6, 5.2.15). The other MSF-02-only supplement fields an op carries
     * are dropped by the MSF-01 re-encode, as an MSF-01 parse ignores those
     * keys. MSF-02: an "add" track carries packaging and isLive (5.2.4.1). */
    for (size_t i = 0; i < delta->delta_update_count; i++) {
        const moq_msf_delta_op_t *op = &delta->delta_update[i];
        if (rev == MOQ_MSF_REVISION_01 && op->op == MOQ_MSF_DELTA_OP_UPDATE)
            return MOQ_ERR_PROTO;
        if (rev == MOQ_MSF_REVISION_02 && op->op == MOQ_MSF_DELTA_OP_ADD)
            for (size_t j = 0; j < op->track_count; j++)
                if (!op->tracks[j].has_packaging || !op->tracks[j].has_is_live)
                    return MOQ_ERR_PROTO;
    }
    if (rev == MOQ_MSF_REVISION_01) {
        for (size_t i = 0; i < delta->delta_update_count; i++) {
            const moq_msf_delta_op_t *op = &delta->delta_update[i];
            for (size_t j = 0; j < op->track_count; j++) {
                const moq_msf_track_supplement_t *sp =
                    moq_msf_supplement_of(&op->tracks[j]);
                if (sp && sp->depends_render_group_count > 0)
                    return MOQ_ERR_PROTO;
            }
        }
    } else {
        /* MSF-02: op tracks follow the naming rules (5.2.2, 5.2.3, 11.2),
         * including the naming keys the parse found malformed. */
        for (size_t i = 0; i < delta->delta_update_count; i++) {
            const moq_msf_delta_op_t *op = &delta->delta_update[i];
            for (size_t j = 0; j < op->track_count; j++) {
                const moq_msf_track_t *ot = &op->tracks[j];
                const moq_msf_track_supplement_t *sp = moq_msf_supplement_of(ot);
                if ((sp && sp->malformed) ||
                    !track_naming_ok_r02(ot, op->op != MOQ_MSF_DELTA_OP_UPDATE,
                                         false))
                    return MOQ_ERR_PROTO;
            }
        }
    }

    moq_msf_track_t *eff = NULL;
    uint8_t         *removed = NULL;
    size_t rem_bytes = 0;
    /* Merged extension lists of clone tracks, kept until the encode. */
    msf_tmp_block_t *merged = NULL;
    size_t merged_n = 0;
    if (clones > 0) {
        merged = (msf_tmp_block_t *)alloc->alloc(merged_bytes, alloc->ctx);
        if (!merged) return MOQ_ERR_NOMEM;
    }
    /* MSF-02 compares Full Track Names by namespace fields, a track without
     * "namespace" inheriting the catalog's (5.2.2, 11.2.2). */
    msf_ident_mode_t mode = { .fields = rev == MOQ_MSF_REVISION_02,
                              .catalog_ns = in->catalog_ns };
    msf_index_t ix = { NULL, 0, 0, mode };
    if (cap > 0) {
        moq_result_t arc = MOQ_ERR_NOMEM;
        if (!(eff = (moq_msf_track_t *)alloc->alloc(eff_bytes, alloc->ctx)) ||
            !moq_msf_checked_array_bytes(cap, sizeof(uint8_t), &rem_bytes) ||
            !(removed = (uint8_t *)alloc->alloc(rem_bytes, alloc->ctx)) ||
            (arc = moq_msf_index_init(&ix, cap, &mode, alloc)) < 0) {
            if (removed) alloc->free(removed, rem_bytes, alloc->ctx);
            if (eff) alloc->free(eff, eff_bytes, alloc->ctx);
            if (merged) alloc->free(merged, merged_bytes, alloc->ctx);
            return arc;
        }
        memset(removed, 0, rem_bytes);
    }

    size_t n = 0;
    for (size_t i = 0; i < base->track_count; i++) {
        eff[n] = base->tracks[i];
        moq_msf_index_insert(&ix, eff, (long)n);
        n++;
    }

    moq_result_t rc = MOQ_OK;
    for (size_t i = 0; i < delta->delta_update_count && rc == MOQ_OK; i++) {
        const moq_msf_delta_op_t *op = &delta->delta_update[i];
        for (size_t j = 0; j < op->track_count; j++) {
            const moq_msf_track_t *ot = &op->tracks[j];
            /* Reject a NULL/sentinel identity span before it is hashed. */
            if (!track_identity_ok(ot) ||
                ((op->op == MOQ_MSF_DELTA_OP_CLONE ||
                  op->op == MOQ_MSF_DELTA_OP_UPDATE) &&
                 (!ident_span_ok(ot->parent_name) ||
                  (ot->has_parent_namespace &&
                   !ident_span_ok(ot->parent_namespace))))) {
                rc = MOQ_ERR_INVAL; break;
            }
            if (op->op == MOQ_MSF_DELTA_OP_ADD) {
                /* 5.1.6/5.3: add declares a NEW tuple -- a collision is a bad
                 * delta (the tuple is immutable once declared). */
                if (moq_msf_index_find(&ix, eff, ot->has_namespace,
                                  ot->namespace_, ot->name) >= 0) {
                    rc = MOQ_ERR_PROTO; break;
                }
                eff[n] = *ot; removed[n] = 0;
                moq_msf_index_insert(&ix, eff, (long)n); n++;
            } else if (op->op == MOQ_MSF_DELTA_OP_REMOVE) {
                long idx = moq_msf_index_find(&ix, eff, ot->has_namespace,
                                         ot->namespace_, ot->name);
                if (idx < 0) { rc = MOQ_ERR_PROTO; break; }  /* not declared */
                removed[idx] = 1;
                moq_msf_index_remove(&ix, eff, ot->has_namespace,
                                ot->namespace_, ot->name);
            } else if (op->op == MOQ_MSF_DELTA_OP_UPDATE) {
                /* MSF-02: change the target in place; its Full Track Name, and
                 * so its index entry and position, stay. */
                long idx = moq_msf_index_find(&ix, eff, ot->has_parent_namespace,
                                              ot->parent_namespace, ot->parent_name);
                if (idx < 0) { rc = MOQ_ERR_PROTO; break; }  /* unknown track */
                moq_msf_track_t e = eff[(size_t)idx];
                if (!update_allowed(&mode, &e, ot)) { rc = MOQ_ERR_PROTO; break; }
                apply_field_overrides(&e, ot, rev, false);
                msf_tmp_block_t *mb = &merged[merged_n];
                rc = clone_merge_ext(alloc, &e, ot, rev, &mb->p, &mb->n);
                if (rc < 0) break;
                if (mb->p) merged_n++;
                eff[(size_t)idx] = e;
            } else { /* CLONE */
                long idx = moq_msf_index_find(&ix, eff, ot->has_parent_namespace,
                                         ot->parent_namespace, ot->parent_name);
                if (idx < 0) { rc = MOQ_ERR_PROTO; break; }  /* parent absent */
                moq_msf_track_t e = eff[(size_t)idx];
                apply_clone_overrides(&e, ot, rev);
                msf_tmp_block_t *mb = &merged[merged_n];
                rc = clone_merge_ext(alloc, &e, ot, rev, &mb->p, &mb->n);
                if (rc < 0) break;
                if (mb->p) merged_n++;
                /* The cloned tuple MUST be new. */
                if (moq_msf_index_find(&ix, eff, e.has_namespace,
                                  e.namespace_, e.name) >= 0) {
                    rc = MOQ_ERR_PROTO; break;
                }
                eff[n] = e; removed[n] = 0;
                moq_msf_index_insert(&ix, eff, (long)n); n++;
            }
        }
    }

    /* Compact: drop tombstoned (removed) slots, preserving order. */
    size_t live = 0;
    if (rc == MOQ_OK)
        for (size_t i = 0; i < n; i++)
            if (!removed[i]) eff[live++] = eff[i];

    moq_msf_index_free(&ix, alloc);
    if (removed) alloc->free(removed, rem_bytes, alloc->ctx);

    moq_rcbuf_t *json = NULL;
    if (rc == MOQ_OK) {
        moq_msf_catalog_t mid;
        memset(&mid, 0, sizeof(mid));
        mid.struct_size = sizeof(mid);
        mid.version = MOQ_MSF_VERSION;
        /* The applied catalog keeps the base's revision, version spelling and
         * publish tracks. */
        mid.revision = rev;
        mid.version_form = catalog_version_form(base);
        const moq_msf_track_t *pt;
        mid.publish_track_count = moq_msf_publish_tracks_of(base, &pt);
        mid.publish_tracks = (moq_msf_track_t *)(uintptr_t)pt;
        mid.tracks = eff;
        mid.track_count = live;
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
         * moq_msf_catalog_encode. Caller storage was checked above, so a
         * catalog this encode refuses is the peer's error. */
        rc = catalog_encode_impl(alloc, &mid, &json, /*strict_cp=*/false);
        if (rc == MOQ_ERR_INVAL) rc = MOQ_ERR_PROTO;
    }

    for (size_t i = 0; i < merged_n; i++)
        alloc->free(merged[i].p, merged[i].n, alloc->ctx);
    if (merged) alloc->free(merged, merged_bytes, alloc->ctx);
    if (eff) alloc->free(eff, eff_bytes, alloc->ctx);
    if (rc < 0) return rc;

    moq_bytes_t jb = { moq_rcbuf_data(json), moq_rcbuf_len(json) };
    rc = parse_impl(alloc, jb, out, out_size);
    moq_rcbuf_decref(json);
    /* MSF-02 5.3: the catalog every op leaves must be valid, with names
     * compared under the catalog namespace the caller gave. */
    if (rc == MOQ_OK && rev == MOQ_MSF_REVISION_02) {
        moq_msf_validate_cfg_t vc;
        moq_msf_validate_cfg_init(&vc);
        vc.catalog_namespace = in->catalog_ns;
        rc = moq_msf_catalog_check(alloc, out, &vc, NULL);
        if (rc == MOQ_ERR_INVAL) rc = MOQ_ERR_PROTO;
        if (rc < 0) moq_msf_catalog_cleanup(alloc, out);
    }
    return rc;
}

/* Every apply entry point: refuse what cannot be written, run, and leave
 * *out zeroed and stamped on any later error. */
static moq_result_t apply_impl(const moq_alloc_t *alloc,
                               const moq_msf_catalog_t *base,
                               const moq_msf_catalog_t *delta,
                               const moq_msf_validate_cfg_t *cfg,
                               apply_in_t *in,
                               moq_msf_catalog_t *out, size_t out_size)
{
    if (!alloc || !base || !delta || !out) return MOQ_ERR_INVAL;
    msf_extent_t ox = moq_msf_extent_of(out_size);
    if (ox == MSF_EXTENT_SHORT || ox == MSF_EXTENT_PARTIAL) return MOQ_ERR_INVAL;
    size_t on = out_size < sizeof(*out) ? out_size : sizeof(*out);
    moq_msf_validate_cfg_t c;
    moq_result_t rc = moq_msf_read_validate_cfg(cfg, &c);
    if (rc == MOQ_OK) {
        if (cfg) {
            in->lim_ops = c.max_delta_ops;
            in->lim_tracks = c.max_effective_tracks;
            in->catalog_ns = c.catalog_namespace;
        }
        rc = apply_run(alloc, base, delta, in, out, out_size);
    }
    if (rc < 0) {
        moq_msf_catalog_t z;
        memset(&z, 0, sizeof(z));
        z.struct_size = (uint32_t)on;
        memcpy(out, &z, on);
    }
    return rc;
}

moq_result_t moq_msf_catalog_apply_delta_ex(const moq_alloc_t *alloc,
                                            const moq_msf_catalog_t *base,
                                            const moq_msf_catalog_t *delta,
                                            const moq_msf_apply_limits_t *limits,
                                            moq_msf_catalog_t *out)
{
    /* ABI-safe limits read: a field is honored only if the caller's struct is
     * large enough to carry it (a zero field is "no limit"). */
    apply_in_t in = { .lim_ops = 0, .lim_tracks = 0,
                      .catalog_ns = { NULL, 0 }, .legacy = true };
    if (limits) {
        if (limits->struct_size >=
            offsetof(moq_msf_apply_limits_t, max_delta_ops) + sizeof(size_t))
            in.lim_ops = limits->max_delta_ops;
        if (limits->struct_size >=
            offsetof(moq_msf_apply_limits_t, max_effective_tracks) + sizeof(size_t))
            in.lim_tracks = limits->max_effective_tracks;
    }
    return apply_impl(alloc, base, delta, NULL, &in, out,
                      MOQ_MSF_CATALOG_SIZE_THROUGH_VERSION_FORM);
}

moq_result_t moq_msf_catalog_apply_delta(const moq_alloc_t *alloc,
                                          const moq_msf_catalog_t *base,
                                          const moq_msf_catalog_t *delta,
                                          moq_msf_catalog_t *out)
{
    /* Unlimited convenience wrapper -- direct callers that trust their peer. */
    return moq_msf_catalog_apply_delta_ex(alloc, base, delta, NULL, out);
}

moq_result_t moq_msf_catalog_apply_delta_sized(const moq_alloc_t *alloc,
                                               const moq_msf_catalog_t *base,
                                               const moq_msf_catalog_t *delta,
                                               const moq_msf_validate_cfg_t *cfg,
                                               moq_msf_catalog_t *out,
                                               size_t out_size)
{
    apply_in_t in = { .lim_ops = 0, .lim_tracks = 0,
                      .catalog_ns = { NULL, 0 }, .legacy = false };
    return apply_impl(alloc, base, delta, cfg, &in, out, out_size);
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

typedef struct msf_enc_buf {
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

static moq_result_t mpeg2ts_fail(moq_msf_mpeg2ts_error_t *reason,
                                 moq_msf_mpeg2ts_error_t why)
{
    if (reason) *reason = why;
    return MOQ_ERR_PROTO;
}

/* The subscriber's check of one mpeg2ts track (see the header). */
static moq_result_t ts_check(const moq_msf_track_t *t,
                             const moq_msf_mpeg2ts_t *m,
                             moq_msf_mpeg2ts_error_t *reason)
{
    /* Security Considerations: values are checked before use. */
    if (m->malformed) return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_MALFORMED_FIELD);

    /* Mode: required; one of the four names; MUST reject an unknown one. */
    if (!m->has_mode) return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_MODE_MISSING);
    if (m->mode < MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM ||
        m->mode > MOQ_MSF_MPEG2TS_MODE_ES_PACKETS)
        return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_MODE_UNRECOGNIZED);
    bool multiplex = m->mode == MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_MULTIPLEX;

    /* Packet Size: required; 188 or 192. */
    if (!m->has_packet_size)
        return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_PACKET_SIZE_MISSING);
    if (m->packet_size != 188 && m->packet_size != 192)
        return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_PACKET_SIZE);

    /* Timestamp Mode: MUST NOT be present when the packet size is 188. */
    if (m->has_timestamp_mode && m->packet_size == 188)
        return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_TIMESTAMP_MODE_188);

    /* ES PID: MUST be present in es-packets and absent otherwise. */
    if (m->mode == MOQ_MSF_MPEG2TS_MODE_ES_PACKETS) {
        if (!m->has_es_pid) return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_ES_PID_MISSING);
    } else if (m->has_es_pid) {
        return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_ES_PID_FORBIDDEN);
    }

    /* Ranges: 13-bit PIDs, 16-bit program numbers (0 names the network PID
     * in a PAT, never a program), a positive rate. */
    if ((m->has_es_pid && m->es_pid > 0x1FFFu) ||
        (m->has_pcr_pid && m->pcr_pid > 0x1FFFu) ||
        (m->has_scte35_pid && m->scte35_pid > 0x1FFFu))
        return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_PID_RANGE);
    for (size_t i = 0; m->has_si_pids && i < m->si_pid_count; i++)
        if (!m->si_pids || m->si_pids[i] > 0x1FFFu)
            return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_PID_RANGE);
    if (m->has_program_number &&
        (m->program_number == 0 || m->program_number > 0xFFFFu))
        return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_PROGRAM_NUMBER_RANGE);
    if (m->has_mux_rate && m->mux_rate == 0)
        return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_MUX_RATE_RANGE);

    /* Mux Rate: MUST be absent in unmodified-multiplex. */
    if (multiplex && m->has_mux_rate)
        return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_MUX_RATE_FORBIDDEN);

    /* SI PIDs: MUST be absent with an ES PID and in unmodified-multiplex;
     * each PID listed once. */
    if (m->has_si_pids && (m->has_es_pid || multiplex))
        return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_SI_PIDS_FORBIDDEN);
    for (size_t i = 0; m->has_si_pids && i < m->si_pid_count; i++)
        for (size_t j = i + 1; j < m->si_pid_count; j++)
            if (m->si_pids[i] == m->si_pids[j])
                return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_SI_PIDS_DUPLICATE);

    /* SCTE-35 PID: same presence rule. */
    if (m->has_scte35_pid && (m->has_es_pid || multiplex))
        return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_SCTE35_PID_FORBIDDEN);

    /* Random Access: in unmodified-multiplex it refers to the reference
     * program and MUST be absent when the catalog names none (a reference
     * program is named by mpeg2tsProgramNumber; a PCR PID alone names
     * none). */
    if (multiplex && m->has_random_access && !m->has_program_number)
        return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_RANDOM_ACCESS_FORBIDDEN);

    /* Switching: an unmodified-multiplex track MUST NOT be in an altGroup. */
    if (multiplex && t->has_alt_group)
        return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_ALT_GROUP_FORBIDDEN);

    return MOQ_OK;
}

moq_result_t moq_msf_track_mpeg2ts_check(const moq_msf_track_t *t,
                                         moq_msf_mpeg2ts_error_t *reason)
{
    if (reason) *reason = MOQ_MSF_MPEG2TS_OK;
    if (!t) return MOQ_ERR_INVAL;
    if (!bytes_eq_lit(t->packaging, "mpeg2ts")) {
        if (reason) *reason = MOQ_MSF_MPEG2TS_ERR_NOT_MPEG2TS;
        return MOQ_ERR_INVAL;
    }
    /* Without the record the track has none of its required fields. */
    static const moq_msf_mpeg2ts_t none;
    const moq_msf_mpeg2ts_t *m = track_ts(t);
    if (m && m->struct_size < MSF_TS_V0) return MOQ_ERR_INVAL;
    return ts_check(t, m ? m : &none, reason);
}

/* Resolve initRef and check the entry: "inline", Base64, whole packets of
 * packet_size each with its sync byte. On MOQ_OK *out (when non-NULL) owns
 * the decoded packets. */
bool moq_msf_ts_whole_packets(const uint8_t *data, size_t len,
                              uint32_t packet_size)
{
    if (packet_size == 0 || len == 0 || len % packet_size != 0) return false;
    size_t sync = packet_size == 192u ? 4u : 0u;
    for (size_t off = 0; off < len; off += packet_size)
        if (data[off + sync] != 0x47u) return false;
    return true;
}

static moq_result_t ts_init_check(const moq_alloc_t *alloc,
                                  const moq_msf_catalog_t *cat,
                                  const moq_msf_track_t *track,
                                  uint32_t packet_size,
                                  moq_rcbuf_t **out,
                                  moq_msf_mpeg2ts_error_t *reason)
{
    const moq_msf_init_data_entry_t *e =
        moq_msf_catalog_find_init_data(cat, track->init_ref);
    if (!e) return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_INIT_REF_UNRESOLVED);
    /* Use of MSF Initialization Data: the entry's type MUST be "inline". */
    if (!bytes_eq_lit(e->type, "inline"))
        return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_INIT_DATA_TYPE);

    moq_rcbuf_t *raw = NULL;
    moq_result_t rc = moq_msf_decode_init_data(alloc, e->data, &raw);
    if (rc == MOQ_ERR_PROTO || rc == MOQ_ERR_INVAL)
        return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_INIT_DATA_BASE64);
    if (rc != MOQ_OK) return rc;

    /* ... and its decoded value MUST be whole source packets of the declared
     * size, each starting with the sync byte. */
    if (!moq_msf_ts_whole_packets(moq_rcbuf_data(raw), moq_rcbuf_len(raw),
                                  packet_size)) {
        moq_rcbuf_decref(raw);
        return mpeg2ts_fail(reason, MOQ_MSF_MPEG2TS_ERR_INIT_DATA_PACKETS);
    }
    if (out) *out = raw;
    else moq_rcbuf_decref(raw);
    return MOQ_OK;
}

moq_result_t moq_msf_track_mpeg2ts_init_data(const moq_alloc_t *alloc,
                                             const moq_msf_catalog_t *cat,
                                             const moq_msf_track_t *track,
                                             moq_rcbuf_t **out_packets,
                                             moq_msf_mpeg2ts_error_t *reason)
{
    if (out_packets) *out_packets = NULL;
    if (reason) *reason = MOQ_MSF_MPEG2TS_OK;
    if (!alloc || !cat || !track) return MOQ_ERR_INVAL;
    moq_result_t rc = moq_msf_track_mpeg2ts_check(track, reason);
    if (rc != MOQ_OK) return rc;
    if (!track->has_init_ref) return MOQ_ERR_INVAL;
    return ts_init_check(alloc, cat, track, track_ts(track)->packet_size,
                         out_packets, reason);
}

/* How many entries a track's "depends" holds. */
static size_t track_depends_count(const moq_msf_track_t *t)
{
    const moq_msf_track_supplement_t *sp = moq_msf_supplement_of(t);
    return t->depends_count + (sp ? sp->depends_render_group_count : 0);
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

    /* The extension list: a known kind at most once, with a sized record
     * whose own storage is coherent. */
    if (!ext_list_ok(t)) return MOQ_ERR_INVAL;
    return MOQ_OK;
}

/* Authoring: a timestamp mode, when present, is one of the two values the
 * draft defines (an unrecognized string is tolerated on receive only). */
static bool ts_mode_authorable(const moq_msf_mpeg2ts_t *m)
{
    return !m->has_timestamp_mode ||
           m->timestamp_mode == MOQ_MSF_MPEG2TS_TIMESTAMP_ARRIVAL_TIME ||
           m->timestamp_mode == MOQ_MSF_MPEG2TS_TIMESTAMP_OPAQUE;
}

/* Authoring check of the mpeg2ts overrides a clone carries, each judged on
 * its own and in the pairs the rules forbid; the inherited fields are judged
 * when the delta is applied. */
static bool ts_overrides_ok(const moq_msf_track_t *t, const moq_msf_mpeg2ts_t *m)
{
    if (m->malformed || !ts_mode_authorable(m)) return false;
    if (m->has_mode && (m->mode < MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM ||
                        m->mode > MOQ_MSF_MPEG2TS_MODE_ES_PACKETS))
        return false;
    if (m->has_packet_size && m->packet_size != 188 && m->packet_size != 192)
        return false;
    if (m->has_timestamp_mode && m->has_packet_size && m->packet_size == 188)
        return false;
    if ((m->has_es_pid && m->es_pid > 0x1FFFu) ||
        (m->has_pcr_pid && m->pcr_pid > 0x1FFFu) ||
        (m->has_scte35_pid && m->scte35_pid > 0x1FFFu))
        return false;
    for (size_t i = 0; m->has_si_pids && i < m->si_pid_count; i++) {
        if (!m->si_pids || m->si_pids[i] > 0x1FFFu) return false;
        for (size_t j = i + 1; j < m->si_pid_count; j++)
            if (m->si_pids[i] == m->si_pids[j]) return false;
    }
    if (m->has_program_number &&
        (m->program_number == 0 || m->program_number > 0xFFFFu))
        return false;
    if (m->has_mux_rate && m->mux_rate == 0) return false;
    if (m->has_mode) {
        bool multiplex = m->mode == MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_MULTIPLEX;
        if (m->mode != MOQ_MSF_MPEG2TS_MODE_ES_PACKETS && m->has_es_pid)
            return false;
        if (multiplex && (m->has_mux_rate || m->has_si_pids ||
                          m->has_scte35_pid || t->has_alt_group))
            return false;
    }
    if (m->has_es_pid && (m->has_si_pids || m->has_scte35_pid)) return false;
    return true;
}

static moq_result_t validate_track(const moq_msf_track_t *t, bool strict,
                                   moq_msf_revision_t rev)
{
    if (!validate_bytes(t->name) || t->name.len == 0) return MOQ_ERR_INVAL;
    if (!validate_bytes(t->packaging) || t->packaging.len == 0) return MOQ_ERR_INVAL;

    moq_result_t rc = validate_track_optionals(t);
    if (rc < 0) return rc;

    /* Each record judged by its kind: strictly when authoring, and by the
     * receive path's rules on apply's re-encode. */
    for (size_t i = 0; i < t->extension_count; i++) {
        const moq_msf_track_ext_t *e = &t->extensions[i];
        rc = moq_msf_ext_kind_ops(e->kind)->validate(moq_msf_ext_record(e), t,
                                                     strict, rev);
        if (rc < 0) return rc;
    }
    /* draft-gregoire-moq-msfts: an authored mpeg2ts track carries its record
     * (Mode and Packet Size are required). */
    if (strict && bytes_eq_lit(t->packaging, "mpeg2ts") && !track_ts(t))
        return MOQ_ERR_INVAL;
    /* MSF-02 authoring: names map to bytes and namespaces split (11.2), and
     * the fields its (packaging, role) requires are present (5.2.4.1). */
    if (strict && rev == MOQ_MSF_REVISION_02 &&
        (!track_naming_ok_r02(t, true, true) || !moq_msf_required_fields_ok(t)))
        return MOQ_ERR_INVAL;

    /* MSF 5.2.35: trackDuration MUST NOT be present on a live track. */
    if (t->has_track_duration && t->is_live) return MOQ_ERR_INVAL;

    /* MSF 5.2.5: eventType is required iff packaging is "eventtimeline". */
    bool is_event_timeline = bytes_eq_lit(t->packaging, "eventtimeline");
    if (is_event_timeline &&
        (!t->has_event_type || t->event_type.len == 0)) return MOQ_ERR_INVAL;
    if (!is_event_timeline && t->has_event_type) return MOQ_ERR_INVAL;

    /* MSF-01 §8.2: an eventtimeline track MUST carry mimeType
     * "application/json" and a non-empty depends list (the tracks it applies
     * to). MSF-02 8.3 asks only for eventType, checked above. mimeType and
     * depends stay optional, free-form metadata for other packaging types. */
    if (is_event_timeline && rev == MOQ_MSF_REVISION_01) {
        if (!t->has_mime_type ||
            !bytes_eq_lit(t->mime_type, "application/json")) return MOQ_ERR_INVAL;
        if (t->depends_count == 0) return MOQ_ERR_INVAL;
    }

    /* §7.2: an explicit media timeline track (packaging "mediatimeline") MUST
     * carry a non-empty depends list naming what it applies to, in both
     * revisions; MSF-01 also requires mimeType "application/json". (eventType
     * is already forbidden above for any non-eventtimeline packaging.) */
    if (bytes_eq_lit(t->packaging, "mediatimeline")) {
        if (rev == MOQ_MSF_REVISION_01 &&
            (!t->has_mime_type ||
             !bytes_eq_lit(t->mime_type, "application/json")))
            return MOQ_ERR_INVAL;
        if (track_depends_count(t) == 0) return MOQ_ERR_INVAL;
    }

    return MOQ_OK;
}

/* Validate a delta op track per op kind (MSF 5.1.6). An "add" track is a full
 * track; a "remove" track carries only name (+optional namespace); a "clone"
 * track carries parentName + a new name + optional overrides. */
static moq_result_t validate_delta_track(moq_msf_delta_op_kind_t op,
                                         const moq_msf_track_t *t,
                                         bool strict, moq_msf_revision_t rev)
{
    /* An update names its target by parentName; its name may be absent. */
    if (!validate_bytes(t->name) ||
        (t->name.len == 0 && op != MOQ_MSF_DELTA_OP_UPDATE))
        return MOQ_ERR_INVAL;
    if (!ext_list_ok(t)) return MOQ_ERR_INVAL;
    const moq_msf_track_supplement_t *sp = moq_msf_supplement_of(t);
    if (sp && moq_msf_supplement_validate(sp, t, strict, rev) < 0)
        return MOQ_ERR_INVAL;
    if (strict && rev == MOQ_MSF_REVISION_02 &&
        !track_naming_ok_r02(t, op != MOQ_MSF_DELTA_OP_UPDATE, true))
        return MOQ_ERR_INVAL;
    const moq_msf_mpeg2ts_t *m = track_ts(t);
    bool has_ts = m != NULL;
    if (op == MOQ_MSF_DELTA_OP_ADD) {
        /* Lenient like the parser: packaging may be omitted in the example;
         * if present it must be non-empty. Optional spans must be coherent. */
        if (t->has_packaging &&
            (!validate_bytes(t->packaging) || t->packaging.len == 0))
            return MOQ_ERR_INVAL;
        moq_result_t rc = validate_track_optionals(t);
        if (rc < 0) return rc;
        /* MSF-02 authoring: an added track is a whole track (5.2.4.1). */
        if (strict && rev == MOQ_MSF_REVISION_02 && t->has_packaging &&
            !moq_msf_required_fields_ok(t))
            return MOQ_ERR_INVAL;
        /* Authoring: mpeg2ts fields make a whole mpeg2ts track. */
        if (strict && has_ts &&
            (!t->has_packaging || !bytes_eq_lit(t->packaging, "mpeg2ts") ||
             ts_check(t, m, NULL) != MOQ_OK || !ts_mode_authorable(m)))
            return MOQ_ERR_INVAL;
        if (strict && t->has_packaging && bytes_eq_lit(t->packaging, "mpeg2ts") &&
            !has_ts)
            return MOQ_ERR_INVAL;            /* Mode and Packet Size missing */
        return MOQ_OK;
    }
    if (op == MOQ_MSF_DELTA_OP_REMOVE) {
        /* MSF 5.1.6: name (+optional namespace) only. */
        if (!track_is_name_namespace_only(t)) return MOQ_ERR_INVAL;
        if (t->has_namespace && !validate_bytes(t->namespace_)) return MOQ_ERR_INVAL;
        return MOQ_OK;
    }
    /* CLONE, UPDATE */
    if (!t->has_parent_name || !validate_bytes(t->parent_name) ||
        t->parent_name.len == 0) return MOQ_ERR_INVAL;
    if (t->has_parent_namespace && !validate_bytes(t->parent_namespace))
        return MOQ_ERR_INVAL;
    if (t->has_packaging &&
        (!validate_bytes(t->packaging) || t->packaging.len == 0))
        return MOQ_ERR_INVAL;
    moq_result_t rc = validate_track_optionals(t);
    if (rc < 0) return rc;
    if (strict && has_ts &&
        ((t->has_packaging && !bytes_eq_lit(t->packaging, "mpeg2ts")) ||
         !ts_overrides_ok(t, m)))
        return MOQ_ERR_INVAL;
    return MOQ_OK;
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

/* Emit the mpeg2ts keys in the order of the draft's field table. A malformed
 * value is written as null and an unrecognized name as itself, so that the
 * receive-path re-encode keeps a peer's invalid track invalid; authoring
 * refuses both before getting here. */
static void enc_track_mpeg2ts(enc_buf_t *b, const moq_msf_mpeg2ts_t *m, bool *f)
{
#define MPEG2TS_BAD(bit) ((m->malformed & (bit)) != 0)
    if (m->has_mode) {
        enc_comma(b, f);
        enc_key(b, "mpeg2tsMode");
        if (MPEG2TS_BAD(MOQ_MSF_MPEG2TS_F_MODE)) {
            enc_lit(b, "null");
        } else if (m->mode != MOQ_MSF_MPEG2TS_MODE_UNRECOGNIZED) {
            const char *nm = moq_msf_mpeg2ts_mode_name(m->mode);
            enc_json_string(b, (const uint8_t *)nm, strlen(nm));
        } else {
            enc_json_string(b, m->mode_name.data, m->mode_name.len);
        }
    }
#define MPEG2TS_NUM(has, bit, key, val) \
    if (m->has) { \
        enc_comma(b, f); \
        enc_key(b, key); \
        if (MPEG2TS_BAD(bit)) enc_lit(b, "null"); else enc_u64_raw(b, (uint64_t)(val)); \
    }
    MPEG2TS_NUM(has_packet_size, MOQ_MSF_MPEG2TS_F_PACKET_SIZE,
                "mpeg2tsPacketSize", m->packet_size)
    MPEG2TS_NUM(has_es_pid, MOQ_MSF_MPEG2TS_F_ES_PID, "mpeg2tsEsPid", m->es_pid)
    MPEG2TS_NUM(has_program_number, MOQ_MSF_MPEG2TS_F_PROGRAM_NUMBER,
                "mpeg2tsProgramNumber", m->program_number)
    MPEG2TS_NUM(has_pcr_pid, MOQ_MSF_MPEG2TS_F_PCR_PID, "mpeg2tsPcrPid", m->pcr_pid)
    MPEG2TS_NUM(has_mux_rate, MOQ_MSF_MPEG2TS_F_MUX_RATE, "mpeg2tsMuxRate",
                m->mux_rate)
    if (m->has_si_pids) {
        enc_comma(b, f);
        enc_key(b, "mpeg2tsSiPids");
        if (MPEG2TS_BAD(MOQ_MSF_MPEG2TS_F_SI_PIDS)) {
            enc_lit(b, "null");
        } else {
            enc_char(b, '[');
            for (size_t i = 0; i < m->si_pid_count; i++) {
                if (i > 0) enc_char(b, ',');
                enc_u64_raw(b, m->si_pids[i]);
            }
            enc_char(b, ']');
        }
    }
    if (m->has_random_access) {
        enc_comma(b, f);
        enc_key(b, "mpeg2tsRandomAccess");
        if (MPEG2TS_BAD(MOQ_MSF_MPEG2TS_F_RANDOM_ACCESS)) enc_lit(b, "null");
        else enc_lit(b, m->random_access ? "true" : "false");
    }
    if (m->has_timestamp_mode) {
        enc_comma(b, f);
        enc_key(b, "mpeg2tsTimestampMode");
        if (MPEG2TS_BAD(MOQ_MSF_MPEG2TS_F_TIMESTAMP_MODE)) {
            enc_lit(b, "null");
        } else if (m->timestamp_mode != MOQ_MSF_MPEG2TS_TIMESTAMP_UNRECOGNIZED) {
            const char *nm = moq_msf_mpeg2ts_timestamp_mode_name(m->timestamp_mode);
            enc_json_string(b, (const uint8_t *)nm, strlen(nm));
        } else {
            enc_json_string(b, m->timestamp_mode_name.data,
                            m->timestamp_mode_name.len);
        }
    }
    MPEG2TS_NUM(has_scte35_pid, MOQ_MSF_MPEG2TS_F_SCTE35_PID, "mpeg2tsScte35Pid",
                m->scte35_pid)
#undef MPEG2TS_NUM
#undef MPEG2TS_BAD
}

/* -- Extension kinds ---------------------------------------------------------
 * The rows of moq_msf_ext_kinds: how each record kind is parsed, merged,
 * judged, emitted, released and compared. */

static void ts_init(void *rec)
{
    moq_msf_mpeg2ts_init_sized((moq_msf_mpeg2ts_t *)rec, sizeof(moq_msf_mpeg2ts_t));
}

static bool ts_key_known(const json_string_t *name, unsigned caps)
{
    (void)caps;   /* both extents and both revisions */
    return name->string_size > 7 && memcmp(name->string, "mpeg2ts", 7) == 0 &&
           mpeg2ts_key_known(name);
}

static moq_result_t ts_parse_key(const moq_alloc_t *alloc,
                                 const json_object_element_t *e, void *rec)
{
    moq_result_t rc = MOQ_OK;
    (void)parse_mpeg2ts_key(alloc, e, (moq_msf_mpeg2ts_t *)rec, &rc);
    return rc;
}

static void ts_merge(void *dst, const void *o, moq_msf_revision_t rev)
{
    (void)rev;
    ts_merge_clone((moq_msf_mpeg2ts_t *)dst, (const moq_msf_mpeg2ts_t *)o);
}

static bool ts_storage_ok(const void *rec)
{
    const moq_msf_mpeg2ts_t *m = (const moq_msf_mpeg2ts_t *)rec;
    if (m->has_si_pids && m->si_pid_count > 0 && !m->si_pids) return false;
    if (m->has_mode && !validate_bytes(m->mode_name)) return false;
    if (m->has_timestamp_mode && !validate_bytes(m->timestamp_mode_name))
        return false;
    return true;
}

/* draft-gregoire-moq-msfts: authoring applies the subscriber's track check,
 * refuses an unrecognized timestamp mode, and keeps mpeg2ts keys off other
 * packagings. The receive-path re-encode keeps what a peer sent. */
static moq_result_t ts_validate(const void *rec, const moq_msf_track_t *t,
                                bool strict, moq_msf_revision_t rev)
{
    (void)rev;
    if (!strict) return MOQ_OK;
    const moq_msf_mpeg2ts_t *m = (const moq_msf_mpeg2ts_t *)rec;
    if (!bytes_eq_lit(t->packaging, "mpeg2ts")) return MOQ_ERR_INVAL;
    if (ts_check(t, m, NULL) != MOQ_OK || !ts_mode_authorable(m))
        return MOQ_ERR_INVAL;
    return MOQ_OK;
}

static void ts_encode(enc_buf_t *b, const void *rec, moq_msf_revision_t rev,
                      bool *f)
{
    (void)rev;
    enc_track_mpeg2ts(b, (const moq_msf_mpeg2ts_t *)rec, f);
}

static void ts_free_owned(const moq_alloc_t *alloc, void *rec)
{
    moq_msf_mpeg2ts_t *m = (moq_msf_mpeg2ts_t *)rec;
    if (m->si_pids)
        alloc->free(m->si_pids, m->si_pid_count * sizeof(uint32_t), alloc->ctx);
    m->si_pids = NULL;
    m->si_pid_count = 0;
}

/* Semantic equality: values, malformed bits, and the names an unrecognized
 * enum value was spelled with. */
static bool ts_equal(const void *pa, const void *pb)
{
    const moq_msf_mpeg2ts_t *a = (const moq_msf_mpeg2ts_t *)pa;
    const moq_msf_mpeg2ts_t *b = (const moq_msf_mpeg2ts_t *)pb;
    if (!a || !b) return a == b;
    if (a->malformed != b->malformed) return false;
#define TS_FIELD(f) \
    if (a->has_##f != b->has_##f || (a->has_##f && a->f != b->f)) return false
    TS_FIELD(mode);
    TS_FIELD(packet_size);
    TS_FIELD(es_pid);
    TS_FIELD(program_number);
    TS_FIELD(pcr_pid);
    TS_FIELD(mux_rate);
    TS_FIELD(random_access);
    TS_FIELD(timestamp_mode);
    TS_FIELD(scte35_pid);
#undef TS_FIELD
    if (a->has_mode && a->mode == MOQ_MSF_MPEG2TS_MODE_UNRECOGNIZED &&
        !bytes_eq(a->mode_name, b->mode_name))
        return false;
    if (a->has_timestamp_mode &&
        a->timestamp_mode == MOQ_MSF_MPEG2TS_TIMESTAMP_UNRECOGNIZED &&
        !bytes_eq(a->timestamp_mode_name, b->timestamp_mode_name))
        return false;
    if (a->has_si_pids != b->has_si_pids) return false;
    if (a->has_si_pids) {
        if (a->si_pid_count != b->si_pid_count) return false;
        for (size_t i = 0; i < a->si_pid_count; i++)
            if (a->si_pids[i] != b->si_pids[i]) return false;
    }
    return true;
}

static bool ts_is_empty(const void *rec)
{
    const moq_msf_mpeg2ts_t *m = (const moq_msf_mpeg2ts_t *)rec;
    return !m->has_mode && !m->has_packet_size && !m->has_es_pid &&
           !m->has_program_number && !m->has_pcr_pid && !m->has_mux_rate &&
           !m->has_si_pids && !m->has_random_access && !m->has_timestamp_mode &&
           !m->has_scte35_pid && !m->malformed;
}

static void supp_init(void *rec)
{
    moq_msf_track_supplement_init_sized((moq_msf_track_supplement_t *)rec,
                                        sizeof(moq_msf_track_supplement_t));
}

/* The supplement's own keys. The encryption keys are MSF-01 and MSF-02
 * fields alike; the rest are MSF-02 vocabulary. A parse into the MSF-01
 * extent reads none of them. ("depends" Numbers and the malformed bits come
 * from the "depends" and naming keys.) */
static bool supp_key_known(const json_string_t *name, unsigned caps)
{
    if (!(caps & MSF_CAP_FULL)) return false;
    if (str_eq(name, "encryptionScheme") || str_eq(name, "cipherSuite") ||
        str_eq(name, "keyId") || str_eq(name, "trackBaseKey"))
        return true;
    if (!(caps & MSF_CAP_R02)) return false;
    return str_eq(name, "packagingMetadata") || str_eq(name, "keyframeSpacing") ||
           str_eq(name, "accessibleWindowTime");
}

/* A lenient read: a value of the wrong JSON type, or a number that is not a
 * non-negative integer, is absent. */
static void supp_span(const json_object_element_t *e, bool *has, moq_bytes_t *v)
{
    *has = get_string(e, v);
    if (!*has) *v = (moq_bytes_t){ NULL, 0 };
}

static void supp_u64(const json_object_element_t *e, bool *has, uint64_t *v)
{
    *has = get_u64(e, v);
    if (!*has) *v = 0;
}

static moq_result_t supp_parse_key(const moq_alloc_t *alloc,
                                   const json_object_element_t *e, void *rec)
{
    (void)alloc;
    moq_msf_track_supplement_t *s = (moq_msf_track_supplement_t *)rec;
    if (str_eq(e->name, "encryptionScheme"))
        supp_span(e, &s->has_encryption_scheme, &s->encryption_scheme);
    else if (str_eq(e->name, "cipherSuite"))
        supp_span(e, &s->has_cipher_suite, &s->cipher_suite);
    else if (str_eq(e->name, "keyId"))
        supp_span(e, &s->has_key_id, &s->key_id);
    else if (str_eq(e->name, "trackBaseKey"))
        supp_span(e, &s->has_track_base_key, &s->track_base_key);
    else if (str_eq(e->name, "packagingMetadata"))
        supp_span(e, &s->has_packaging_metadata, &s->packaging_metadata);
    else if (str_eq(e->name, "keyframeSpacing"))
        supp_u64(e, &s->has_keyframe_spacing, &s->keyframe_spacing);
    else if (str_eq(e->name, "accessibleWindowTime"))
        supp_u64(e, &s->has_accessible_window_ms, &s->accessible_window_ms);
    return MOQ_OK;
}

static void supp_merge(void *dst, const void *o, moq_msf_revision_t rev)
{
    moq_msf_supplement_merge((moq_msf_track_supplement_t *)dst,
                             (const moq_msf_track_supplement_t *)o, rev);
}

static bool supp_storage_ok(const void *rec)
{
    return moq_msf_supplement_storage_ok((const moq_msf_track_supplement_t *)rec);
}

static moq_result_t supp_validate(const void *rec, const moq_msf_track_t *t,
                                  bool strict, moq_msf_revision_t rev)
{
    return moq_msf_supplement_validate((const moq_msf_track_supplement_t *)rec,
                                       t, strict, rev);
}

/* The supplement's scalar keys (its "depends" Numbers go with the String
 * entries). An MSF-01 catalog has none of the MSF-02 keys: the receive-path
 * re-encode drops them, authoring has refused them. */
static void supp_encode(enc_buf_t *b, const void *rec, moq_msf_revision_t rev,
                        bool *f)
{
    const moq_msf_track_supplement_t *s = (const moq_msf_track_supplement_t *)rec;
    if (rev == MOQ_MSF_REVISION_02) {
        if (s->has_packaging_metadata) {
            enc_comma(b, f);
            enc_key_string(b, "packagingMetadata", s->packaging_metadata);
        }
        if (s->has_keyframe_spacing) {
            enc_comma(b, f);
            enc_key_u64(b, "keyframeSpacing", s->keyframe_spacing);
        }
        if (s->has_accessible_window_ms) {
            enc_comma(b, f);
            enc_key_u64(b, "accessibleWindowTime", s->accessible_window_ms);
        }
    }
    if (s->has_encryption_scheme) {
        enc_comma(b, f);
        enc_key_string(b, "encryptionScheme", s->encryption_scheme);
    }
    if (s->has_cipher_suite) {
        enc_comma(b, f);
        enc_key_string(b, "cipherSuite", s->cipher_suite);
    }
    if (s->has_key_id) {
        enc_comma(b, f);
        enc_key_string(b, "keyId", s->key_id);
    }
    if (s->has_track_base_key) {
        enc_comma(b, f);
        enc_key_string(b, "trackBaseKey", s->track_base_key);
    }
}

static void supp_free_owned(const moq_alloc_t *alloc, void *rec)
{
    moq_msf_track_supplement_t *s = (moq_msf_track_supplement_t *)rec;
    if (s->depends_render_groups)
        alloc->free((void *)(uintptr_t)s->depends_render_groups,
                    s->depends_render_group_count * sizeof(int), alloc->ctx);
    s->depends_render_groups = NULL;
    s->depends_render_group_count = 0;
}

static bool supp_equal(const void *a, const void *b)
{
    return moq_msf_supplement_equal((const moq_msf_track_supplement_t *)a,
                                    (const moq_msf_track_supplement_t *)b);
}

static bool supp_is_empty(const void *rec)
{
    return moq_msf_supplement_is_empty((const moq_msf_track_supplement_t *)rec);
}

/* In ascending kind order; the parse block holds the records at the same
 * rows (MSF_ROW_*). */
const msf_ext_kind_ops_t moq_msf_ext_kinds[] = {
    {
        .kind = MOQ_MSF_EXT_MPEG2TS,
        .record_size = sizeof(moq_msf_mpeg2ts_t),
        .v0_size = MOQ_MSF_MPEG2TS_V0_SIZE,
        .init = ts_init,
        .key_known = ts_key_known,
        .parse_key = ts_parse_key,
        .merge = ts_merge,
        .storage_ok = ts_storage_ok,
        .validate = ts_validate,
        .encode = ts_encode,
        .free_owned = ts_free_owned,
        .equal = ts_equal,
        .is_empty = ts_is_empty,
    },
    {
        .kind = MOQ_MSF_EXT_SUPPLEMENT,
        .record_size = sizeof(moq_msf_track_supplement_t),
        .v0_size = MOQ_MSF_TRACK_SUPPLEMENT_V0_SIZE,
        .init = supp_init,
        .key_known = supp_key_known,
        .parse_key = supp_parse_key,
        .merge = supp_merge,
        .storage_ok = supp_storage_ok,
        .validate = supp_validate,
        .encode = supp_encode,
        .free_owned = supp_free_owned,
        .equal = supp_equal,
        .is_empty = supp_is_empty,
    },
};
const size_t moq_msf_ext_kind_count =
    sizeof(moq_msf_ext_kinds) / sizeof(moq_msf_ext_kinds[0]);

_Static_assert(sizeof(moq_msf_ext_kinds) / sizeof(moq_msf_ext_kinds[0]) ==
               MSF_EXT_ROWS, "one parse-block record per kind");

const msf_ext_kind_ops_t *moq_msf_ext_kind_ops(moq_msf_ext_kind_t kind)
{
    for (size_t r = 0; r < moq_msf_ext_kind_count; r++)
        if (moq_msf_ext_kinds[r].kind == kind) return &moq_msf_ext_kinds[r];
    return NULL;
}

/* Emit the optional (has_*-gated) track fields, managing the leading comma via
 * *f. Does NOT emit name / packaging / isLive / parent fields. */
static void enc_track_optionals(enc_buf_t *b, const moq_msf_track_t *t, bool *f,
                                moq_msf_revision_t rev)
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
    /* One "depends" array: the String entries, then (MSF-02) the Number
     * entries. An op track that carried the key emits it even when empty, so
     * that it still clears the inherited lists. */
    const moq_msf_track_supplement_t *sp = moq_msf_supplement_of(t);
    size_t ngroups = sp && rev == MOQ_MSF_REVISION_02
                         ? sp->depends_render_group_count : 0;
    if (t->depends_count > 0 || ngroups > 0 || (sp && sp->has_depends)) {
        enc_comma(b, f);
        enc_key(b, "depends");
        enc_char(b, '[');
        for (size_t i = 0; i < t->depends_count; i++) {
            if (i > 0) enc_char(b, ',');
            enc_json_string(b, t->depends[i].data, t->depends[i].len);
        }
        for (size_t i = 0; i < ngroups; i++) {
            if (i > 0 || t->depends_count > 0) enc_char(b, ',');
            char num[16];
            snprintf(num, sizeof(num), "%d", sp->depends_render_groups[i]);
            enc_lit(b, num);
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
    /* Extension records, in kind order. */
    for (size_t r = 0; r < moq_msf_ext_kind_count; r++) {
        const moq_msf_track_ext_t *e = moq_msf_ext_find(
            t->extensions, t->extension_count, moq_msf_ext_kinds[r].kind);
        if (e) moq_msf_ext_kinds[r].encode(b, moq_msf_ext_record(e), rev, f);
    }
}

static void enc_track(enc_buf_t *b, const moq_msf_track_t *t,
                      moq_msf_revision_t rev)
{
    enc_char(b, '{');
    bool f = true;

    enc_comma(b, &f); enc_key_string(b, "name", t->name);
    enc_comma(b, &f); enc_key_string(b, "packaging", t->packaging);
    enc_comma(b, &f); enc_key_bool(b, "isLive", t->is_live);
    enc_track_optionals(b, t, &f, rev);
    enc_char(b, '}');
}

/* Emit a delta op track object per op kind (MSF 5.1.6). */
static void enc_delta_op_track(enc_buf_t *b, moq_msf_delta_op_kind_t op,
                               const moq_msf_track_t *t, moq_msf_revision_t rev)
{
    enc_char(b, '{');
    bool f = true;

    if (op == MOQ_MSF_DELTA_OP_REMOVE) {
        enc_comma(b, &f); enc_key_string(b, "name", t->name);
        if (t->has_namespace) { enc_comma(b, &f); enc_key_string(b, "namespace", t->namespace_); }
        enc_char(b, '}');
        return;
    }
    if (op == MOQ_MSF_DELTA_OP_UPDATE) {
        enc_comma(b, &f); enc_key_string(b, "parentName", t->parent_name);
        if (t->has_parent_namespace) { enc_comma(b, &f); enc_key_string(b, "parentNamespace", t->parent_namespace); }
        if (t->name.len > 0) { enc_comma(b, &f); enc_key_string(b, "name", t->name); }
        if (t->has_packaging) { enc_comma(b, &f); enc_key_string(b, "packaging", t->packaging); }
        if (t->has_is_live)   { enc_comma(b, &f); enc_key_bool(b, "isLive", t->is_live); }
        enc_track_optionals(b, t, &f, rev);
        enc_char(b, '}');
        return;
    }
    if (op == MOQ_MSF_DELTA_OP_CLONE) {
        enc_comma(b, &f); enc_key_string(b, "parentName", t->parent_name);
        if (t->has_parent_namespace) { enc_comma(b, &f); enc_key_string(b, "parentNamespace", t->parent_namespace); }
        enc_comma(b, &f); enc_key_string(b, "name", t->name);
        if (t->has_packaging) { enc_comma(b, &f); enc_key_string(b, "packaging", t->packaging); }
        if (t->has_is_live)   { enc_comma(b, &f); enc_key_bool(b, "isLive", t->is_live); }
        enc_track_optionals(b, t, &f, rev);
        enc_char(b, '}');
        return;
    }
    /* ADD: a (possibly partial) track object; packaging/isLive emitted only
     * when present so a §5.6.4-style add round-trips. */
    enc_comma(b, &f); enc_key_string(b, "name", t->name);
    if (t->has_packaging) { enc_comma(b, &f); enc_key_string(b, "packaging", t->packaging); }
    if (t->has_is_live)   { enc_comma(b, &f); enc_key_bool(b, "isLive", t->is_live); }
    enc_track_optionals(b, t, &f, rev);
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
    case MOQ_MSF_DELTA_OP_UPDATE: return "update";
    }
    return "add";
}

static size_t enc_catalog_to(uint8_t *buf, size_t cap,
                              const moq_msf_catalog_t *cat,
                              moq_msf_revision_t rev)
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
                enc_delta_op_track(&b, op->op, &op->tracks[j], rev);
            }
            enc_char(&b, ']');
            enc_char(&b, '}');
        }
        enc_char(&b, ']');
        enc_char(&b, '}');
        if (buf && b.overflow) return 0;
        return b.pos;
    }

    /* MSF-01 §5.1.1: version is a JSON String. By default the numeral "1",
     * the wire form strict MSF-01 receivers (PlayA) accept and the form libmoq
     * has emitted cross-impl (E2E case D captured fixture). A caller that
     * selects MOQ_MSF_VERSION_FORM_DRAFT_NAME gets 5.1.1's draft-name
     * convention, "draft-01". */
    if (rev == MOQ_MSF_REVISION_02) {
        /* MSF-02 5.1.1: the one spelling, "draft-02". */
        enc_lit(&b, "{\"version\":\"draft-02\"");
    } else {
        bool draft_name =
            catalog_version_form(cat) == MOQ_MSF_VERSION_FORM_DRAFT_NAME;
        enc_lit(&b, draft_name ? "{\"version\":\"draft-" : "{\"version\":\"");
        { char num[8];
          snprintf(num, sizeof(num), draft_name ? "%02d" : "%d", cat->version);
          enc_lit(&b, num); }
        enc_char(&b, '"');
    }

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
        enc_track(&b, &cat->tracks[i], rev);
    }
    enc_char(&b, ']');

    /* MSF 5.1.5 publishTracks, between tracks and initDataList. */
    const moq_msf_track_t *pt;
    size_t pn = moq_msf_publish_tracks_of(cat, &pt);
    if (pn > 0) {
        enc_lit(&b, ",\"publishTracks\":[");
        for (size_t i = 0; i < pn; i++) {
            if (i > 0) enc_char(&b, ',');
            enc_track(&b, &pt[i], rev);
        }
        enc_char(&b, ']');
    }

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

/* Authoring: Full Track Names are unique within one track array (MSF 5.2.3):
 * within tracks and within publishTracks, not across them (an event timeline
 * can be in both, MSF-02 8). */
static bool tracks_unique(const moq_msf_track_t *t, size_t n,
                          moq_msf_revision_t rev)
{
    /* MSF-02 compares namespace fields (11.2.2); with no catalog namespace
     * an inherited name is its own class, as when authoring one. */
    msf_ident_mode_t mode = { .fields = rev == MOQ_MSF_REVISION_02,
                              .catalog_ns = { NULL, 0 } };
    for (size_t i = 0; i < n; i++)
        for (size_t j = i + 1; j < n; j++)
            if (moq_msf_ident_eq(&mode, t[i].has_namespace, t[i].namespace_,
                                 t[i].name, t[j].has_namespace,
                                 t[j].namespace_, t[j].name))
                return false;
    return true;
}

static moq_result_t catalog_encode_impl(const moq_alloc_t *alloc,
                                        const moq_msf_catalog_t *cat,
                                        moq_rcbuf_t **out_json, bool strict_cp)
{
    if (!alloc || !cat || !out_json) return MOQ_ERR_INVAL;
    *out_json = NULL;

    /* The MSF-02 block is read whole or not at all. */
    if (moq_msf_extent_of(cat->struct_size) == MSF_EXTENT_PARTIAL)
        return MOQ_ERR_INVAL;
    moq_msf_revision_t rev = moq_msf_catalog_revision_of(cat);
    if (rev != MOQ_MSF_REVISION_01 && rev != MOQ_MSF_REVISION_02)
        return MOQ_ERR_INVAL;

    /* The version form is a closed authoring input: absent (a caller struct
     * that ends before it) or NUMERAL selects "1", DRAFT_NAME selects
     * "draft-01", anything else is rejected rather than spelled as "1". An
     * MSF-02 catalog has the one spelling and ignores the field. */
    if (rev == MOQ_MSF_REVISION_01 && catalog_has_version_form(cat) &&
        cat->version_form != MOQ_MSF_VERSION_FORM_NUMERAL &&
        cat->version_form != MOQ_MSF_VERSION_FORM_DRAFT_NAME)
        return MOQ_ERR_INVAL;

    const moq_msf_track_t *pt;
    size_t pn = moq_msf_publish_tracks_of(cat, &pt);

    /* A delta catalog (MSF 5.1.6/5.3): no version/tracks; >= 1 op, each with
     * >= 1 op-validated track. */
    if (cat->delta_update) {
        if (cat->track_count > 0 || cat->delta_update_count == 0)
            return MOQ_ERR_INVAL;
        /* MSF 5.1.3: a delta never carries completion; MSF 5.3: nor
         * publishTracks. */
        if (cat->is_complete || pn > 0) return MOQ_ERR_INVAL;
        /* MSF-02 5.3: a delta carries generatedAt and no other root field
         * than deltaUpdate, so authoring refuses root data it would otherwise
         * leave out, and a parsed delta's root keys however empty. MSF-01
         * deltas keep being written without it. */
        if (rev == MOQ_MSF_REVISION_02 &&
            (!cat->has_generated_at || cat->init_data_count > 0 ||
             cat->content_protection_count > 0))
            return MOQ_ERR_INVAL;
        const msf_dom_hdr_t *hdr = moq_msf_dom_hdr(cat);
        if (rev == MOQ_MSF_REVISION_02 && hdr &&
            (hdr->root_keys & ~(uint32_t)(MSF_ROOT_GENERATED_AT |
                                          MSF_ROOT_DELTA_UPDATE)) != 0)
            return MOQ_ERR_INVAL;
        for (size_t i = 0; i < cat->delta_update_count; i++) {
            const moq_msf_delta_op_t *op = &cat->delta_update[i];
            /* Reject an unknown op kind before encoding it (API footgun for
             * callers building a catalog directly). */
            if (op->op != MOQ_MSF_DELTA_OP_ADD &&
                op->op != MOQ_MSF_DELTA_OP_REMOVE &&
                op->op != MOQ_MSF_DELTA_OP_CLONE &&
                op->op != MOQ_MSF_DELTA_OP_UPDATE) return MOQ_ERR_INVAL;
            /* "update" is MSF-02 vocabulary. */
            if (op->op == MOQ_MSF_DELTA_OP_UPDATE && rev != MOQ_MSF_REVISION_02)
                return MOQ_ERR_INVAL;
            if (op->track_count == 0 || !op->tracks) return MOQ_ERR_INVAL;
            for (size_t j = 0; j < op->track_count; j++) {
                moq_result_t rc = validate_delta_track(op->op, &op->tracks[j],
                                                       strict_cp, rev);
                if (rc < 0) return rc;
            }
        }
        size_t dn = enc_catalog_to(NULL, 0, cat, rev);
        if (dn == 0) return MOQ_ERR_INVAL;
        uint8_t *dbuf = (uint8_t *)alloc->alloc(dn, alloc->ctx);
        if (!dbuf) return MOQ_ERR_NOMEM;
        size_t dw = enc_catalog_to(dbuf, dn, cat, rev);
        if (dw != dn) { alloc->free(dbuf, dn, alloc->ctx); return MOQ_ERR_INVAL; }
        moq_result_t rc = moq_rcbuf_create(alloc, dbuf, dn, out_json);
        alloc->free(dbuf, dn, alloc->ctx);
        return rc;
    }

    if (cat->version != MOQ_MSF_VERSION) return MOQ_ERR_INVAL;
    if (cat->track_count > 0 && !cat->tracks) return MOQ_ERR_INVAL;
    if (pn > 0 && !pt) return MOQ_ERR_INVAL;

    for (size_t i = 0; i < cat->track_count; i++) {
        moq_result_t rc = validate_track(&cat->tracks[i], strict_cp, rev);
        if (rc < 0) return rc;
    }
    /* MSF 5.1.5: a publish track has the structure of a track. */
    for (size_t i = 0; i < pn; i++) {
        moq_result_t rc = validate_track(&pt[i], strict_cp, rev);
        if (rc < 0) return rc;
    }
    /* MSF §5.2.3: a track name is unique per namespace within the catalog.
     * Authoring only (strict_cp) -- the lenient receive re-encode preserves
     * whatever the peer sent, matching the contentProtection refID rule below. */
    if (strict_cp && (!tracks_unique(cat->tracks, cat->track_count, rev) ||
                      !tracks_unique(pt, pn, rev)))
        return MOQ_ERR_INVAL;
    /* MSF-02 5.2.45: one accessibleWindowTime per alternate group. */
    if (strict_cp && rev == MOQ_MSF_REVISION_02 &&
        !moq_msf_alt_group_windows_ok(cat->tracks, cat->track_count))
        return MOQ_ERR_INVAL;
    /* MSF-02 authoring writes only what the inbound check accepts: unique
     * names and resolving "depends" entries (5.2.15). */
    if (strict_cp && rev == MOQ_MSF_REVISION_02) {
        moq_result_t crc = moq_msf_catalog_check(alloc, cat, NULL, NULL);
        if (crc == MOQ_ERR_NOMEM) return crc;
        if (crc != MOQ_OK) return MOQ_ERR_INVAL;
    }

    if (cat->init_data_count > 0 && !cat->init_data_list) return MOQ_ERR_INVAL;
    for (size_t i = 0; i < cat->init_data_count; i++) {
        const moq_msf_init_data_entry_t *e = &cat->init_data_list[i];
        if (!validate_bytes(e->id) || e->id.len == 0) return MOQ_ERR_INVAL;
        if (!validate_bytes(e->type) || e->type.len == 0) return MOQ_ERR_INVAL;
        if (!validate_bytes(e->data)) return MOQ_ERR_INVAL;
        /* MSF-02 5.1.7 authoring: one of the three types, a property type
         * in hex. */
        if (strict_cp && rev == MOQ_MSF_REVISION_02 &&
            !moq_msf_init_entry_authorable_r02(e))
            return MOQ_ERR_INVAL;
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

    /* Use of MSF Initialization Data: an authored mpeg2ts track's initRef
     * names an "inline" entry of whole source packets. */
    if (strict_cp)
        for (size_t i = 0; i < cat->track_count; i++) {
            const moq_msf_track_t *t = &cat->tracks[i];
            if (!t->has_init_ref || !bytes_eq_lit(t->packaging, "mpeg2ts"))
                continue;
            moq_result_t rc = ts_init_check(alloc, cat, t,
                                            track_ts(t)->packet_size, NULL, NULL);
            if (rc == MOQ_ERR_NOMEM) return rc;
            if (rc != MOQ_OK) return MOQ_ERR_INVAL;
        }

    /* Measure pass: buf=NULL, counts bytes needed. */
    size_t needed = enc_catalog_to(NULL, 0, cat, rev);
    if (needed == 0) return MOQ_ERR_INVAL;

    uint8_t *buf = (uint8_t *)alloc->alloc(needed, alloc->ctx);
    if (!buf) return MOQ_ERR_NOMEM;

    size_t written = enc_catalog_to(buf, needed, cat, rev);
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
