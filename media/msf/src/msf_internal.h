#ifndef MOQ_MSF_INTERNAL_H
#define MOQ_MSF_INTERNAL_H

/*
 * Functions of moq-msf that are not part of its public API, shared between
 * its sources. They are not exported from a shared build.
 */

#include <moq/msf.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Whether `len` octets at `data` are whole MPEG-2 TS source packets of
 * `packet_size` octets (188, or 192 with a 4-octet prefix), each with its
 * sync byte, and at least one: the rule draft-gregoire-moq-msfts sets for
 * an mpeg2ts track's initialization data. */
bool moq_msf_ts_whole_packets(const uint8_t *data, size_t len,
                              uint32_t packet_size);

/* count * elem, or false (nothing written) when the product wraps size_t. */
bool moq_msf_checked_array_bytes(size_t count, size_t elem, size_t *out_bytes);

/* -- Catalog extents --------------------------------------------------------
 * What a catalog struct size (struct_size, or out_size on a sized entry
 * point) covers. */
typedef enum msf_extent {
    MSF_EXTENT_SHORT = 0,   /* below MOQ_MSF_CATALOG_V0_SIZE */
    MSF_EXTENT_R01,         /* the MSF-01 extent: short of `revision` */
    MSF_EXTENT_PARTIAL,     /* part of the block at `revision`: refused */
    MSF_EXTENT_FULL,        /* the whole block */
} msf_extent_t;

msf_extent_t moq_msf_extent_of(size_t size);

/* The revision of a catalog whose extent is not PARTIAL: its `revision`
 * field when the extent is FULL, else REVISION_01. */
moq_msf_revision_t moq_msf_catalog_revision_of(const moq_msf_catalog_t *cat);

/* A catalog's publish tracks, read only in the FULL extent. */
size_t moq_msf_publish_tracks_of(const moq_msf_catalog_t *cat,
                                 const moq_msf_track_t **tracks);

/* -- What a parsed document carried ----------------------------------------
 * A parse places a small header at the start of the catalog-owned DOM block
 * (`_dom`), recording which root keys the JSON held whatever their values:
 * apply judges a parsed MSF-02 delta by it (5.3), since the public fields
 * cannot say that an empty or malformed key was present. */
#define MSF_ROOT_VERSION            (1u << 0)
#define MSF_ROOT_GENERATED_AT       (1u << 1)
#define MSF_ROOT_IS_COMPLETE        (1u << 2)
#define MSF_ROOT_TRACKS             (1u << 3)
#define MSF_ROOT_PUBLISH_TRACKS     (1u << 4)
#define MSF_ROOT_DELTA_UPDATE       (1u << 5)
#define MSF_ROOT_INIT_DATA_LIST     (1u << 6)
#define MSF_ROOT_CONTENT_PROTECTIONS (1u << 7)

typedef struct msf_dom_hdr {
    uint32_t magic;
    uint32_t root_keys;   /* MSF_ROOT_* */
} msf_dom_hdr_t;

/* The header of a parsed catalog, or NULL for one a caller built. */
const msf_dom_hdr_t *moq_msf_dom_hdr(const moq_msf_catalog_t *cat);

/* -- Track naming (msf_names.c) --------------------------------------------- */

/* Whether `len` octets are well-formed UTF-8 encoding only Unicode scalar
 * values (MSF-02 11.2.1). */
bool moq_msf_utf8_valid(const uint8_t *s, size_t len);

/* Whether a "namespace" string splits into 1..32 non-empty fields (5.2.2). */
bool moq_msf_namespace_syntax_ok(moq_bytes_t ns);

/* How Full Track Names compare. MSF-01: the name and the namespace string
 * as written, a track without one matching only another without one.
 * MSF-02 (fields): the name bytes and the namespace fields (11.2.2), a track
 * without "namespace" inheriting catalog_ns; with catalog_ns unknown (count
 * 0) it matches only another track without one. */
typedef struct msf_ident_mode {
    bool            fields;
    moq_namespace_t catalog_ns;
} msf_ident_mode_t;

bool moq_msf_ident_eq(const msf_ident_mode_t *m,
                      bool a_has, moq_bytes_t a_ns, moq_bytes_t a_name,
                      bool b_has, moq_bytes_t b_ns, moq_bytes_t b_name);

/* An open-addressing set of track indexes keyed on Full Track Name. */
typedef struct msf_index {
    long            *slot;
    size_t           cap;    /* a power of two */
    size_t           mask;
    msf_ident_mode_t mode;
} msf_index_t;

moq_result_t moq_msf_index_init(msf_index_t *ix, size_t n,
                                const msf_ident_mode_t *mode,
                                const moq_alloc_t *alloc);
void moq_msf_index_free(msf_index_t *ix, const moq_alloc_t *alloc);
/* The index of the live track named (has_ns, ns, name), or -1. */
long moq_msf_index_find(const msf_index_t *ix, const moq_msf_track_t *tracks,
                        bool has_ns, moq_bytes_t ns, moq_bytes_t name);
/* Insert tracks[ti]; the caller has confirmed its name is absent. */
void moq_msf_index_insert(msf_index_t *ix, const moq_msf_track_t *tracks,
                          long ti);
void moq_msf_index_remove(msf_index_t *ix, const moq_msf_track_t *tracks,
                          bool has_ns, moq_bytes_t ns, moq_bytes_t name);

/* An open-addressing count of byte-span keys (borrowed for its life). */
typedef struct msf_span_slot {
    moq_bytes_t key;      /* len SIZE_MAX: empty */
    size_t      count;
} msf_span_slot_t;

typedef struct msf_span_index {
    msf_span_slot_t *slot;
    size_t           cap;
    size_t           mask;
} msf_span_index_t;

moq_result_t moq_msf_span_index_init(msf_span_index_t *ix, size_t n,
                                     const moq_alloc_t *alloc);
void moq_msf_span_index_free(msf_span_index_t *ix, const moq_alloc_t *alloc);
/* Add one occurrence of k; the count after adding it. */
size_t moq_msf_span_index_add(msf_span_index_t *ix, moq_bytes_t k);
size_t moq_msf_span_index_count(const msf_span_index_t *ix, moq_bytes_t k);

/* -- Validate cfg ------------------------------------------------------------ */

/* A caller's cfg read through its own extent into *local (NULL: defaults).
 * MOQ_ERR_INVAL below the first published size or for an unusable catalog
 * namespace. */
moq_result_t moq_msf_read_validate_cfg(const moq_msf_validate_cfg_t *cfg,
                                       moq_msf_validate_cfg_t *local);

/* -- The track supplement (msf_supplement.c) --------------------------------- */

/* A track's supplement when it carries a usable one (at least its first
 * published size), else NULL. */
const moq_msf_track_supplement_t *moq_msf_supplement_of(const moq_msf_track_t *t);
/* Whether it holds a field only MSF-02 defines. */
bool moq_msf_supplement_has_r02_fields(const moq_msf_track_supplement_t *s);
bool moq_msf_supplement_is_empty(const moq_msf_track_supplement_t *s);
void moq_msf_supplement_merge(moq_msf_track_supplement_t *dst,
                              const moq_msf_track_supplement_t *o,
                              moq_msf_revision_t rev);
bool moq_msf_supplement_storage_ok(const moq_msf_track_supplement_t *s);
moq_result_t moq_msf_supplement_validate(const moq_msf_track_supplement_t *s,
                                         const moq_msf_track_t *t, bool strict,
                                         moq_msf_revision_t rev);
bool moq_msf_supplement_equal(const moq_msf_track_supplement_t *a,
                              const moq_msf_track_supplement_t *b);

/* MSF-02 authoring rules beyond one record: the fields 5.2.4.1 requires of
 * a track's (packaging, role); one accessibleWindowTime per altGroup
 * (5.2.45); an initDataList entry's type and property-type syntax (5.1.7). */
bool moq_msf_required_fields_ok(const moq_msf_track_t *t);
bool moq_msf_alt_group_windows_ok(const moq_msf_track_t *t, size_t n);
bool moq_msf_init_entry_authorable_r02(const moq_msf_init_data_entry_t *e);

/* -- Extension kinds -----------------------------------------------------------
 * One row per kind this library models, in ascending kind order. A new kind
 * is a row, its record type and its record in the parse block. The JSON and
 * encoder types stay private to msf.c, so they are named here only. */
struct json_string_s;
struct json_object_element_s;
struct msf_enc_buf;

#define MSF_CAP_FULL (1u << 0)  /* the full extent: supplement records */
#define MSF_CAP_R02  (1u << 1)  /* MSF-02-only keys are read */

typedef struct msf_ext_kind_ops {
    moq_msf_ext_kind_t kind;
    size_t record_size;
    size_t v0_size;
    void (*init)(void *rec);
    /* Whether a track key belongs to this kind under the parse's caps. */
    bool (*key_known)(const struct json_string_s *name, unsigned caps);
    moq_result_t (*parse_key)(const moq_alloc_t *alloc,
                              const struct json_object_element_s *e,
                              void *rec);
    /* Fold an op track's record over the inherited one (clone, update). */
    void (*merge)(void *dst, const void *override_rec, moq_msf_revision_t rev);
    /* Caller storage a parse never gets wrong (spans, arrays). */
    bool (*storage_ok)(const void *rec);
    /* The record judged on its track: strict for authoring, else the
     * receive path. */
    moq_result_t (*validate)(const void *rec, const moq_msf_track_t *track,
                             bool strict, moq_msf_revision_t rev);
    /* Emit the record's keys, judged already, for revision rev. */
    void (*encode)(struct msf_enc_buf *b, const void *rec,
                   moq_msf_revision_t rev, bool *first);
    void (*free_owned)(const moq_alloc_t *alloc, void *rec);
    bool (*equal)(const void *a, const void *b);
    bool (*is_empty)(const void *rec);
} msf_ext_kind_ops_t;

extern const msf_ext_kind_ops_t moq_msf_ext_kinds[];
extern const size_t moq_msf_ext_kind_count;

/* The row of a kind, or NULL for one this library does not model. */
const msf_ext_kind_ops_t *moq_msf_ext_kind_ops(moq_msf_ext_kind_t kind);
/* An entry's record, whatever its kind (NULL for an unknown kind). */
const void *moq_msf_ext_record(const moq_msf_track_ext_t *e);

#endif
