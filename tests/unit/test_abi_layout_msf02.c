/*
 * Layout pins for the MSF catalog structs the MSF-02 surface extended or
 * added.
 *
 * moq_msf_catalog_t is pinned at the boundary of the layout it extends: the
 * layout through version_form, which a caller built against it passes as
 * struct_size, re-declared here field by field. Every appended field starts
 * at or past that layout's sizeof, so none sits in a caller's tail padding,
 * and the block that starts at `revision` ends the struct.
 *
 * The pins hold on every data model: compiled here for the host, and
 * freestanding by scripts/check_ilp32_headers.sh for two ILP32 targets.
 */
#include <moq/msf.h>

#include <stddef.h>
#include <stdint.h>

/* The catalog as it was through version_form. */
struct msf_catalog_through_version_form {
    uint32_t                       struct_size;
    int                            version;
    moq_msf_track_t               *tracks;
    size_t                         track_count;
    bool                           has_generated_at;
    uint64_t                       generated_at;
    bool                           is_complete;
    moq_msf_init_data_entry_t     *init_data_list;
    size_t                         init_data_count;
    moq_cmsf_content_protection_t *content_protections;
    size_t                         content_protection_count;
    moq_msf_delta_op_t            *delta_update;
    size_t                         delta_update_count;
    void                          *_dom;
    size_t                         _dom_size;
    moq_msf_version_form_t         version_form;
};

#define OLD struct msf_catalog_through_version_form
#define SAME(f) _Static_assert(offsetof(moq_msf_catalog_t, f) == offsetof(OLD, f), \
                               "moq_msf_catalog_t." #f " did not move")
SAME(struct_size);
SAME(version);
SAME(tracks);
SAME(track_count);
SAME(has_generated_at);
SAME(generated_at);
SAME(is_complete);
SAME(init_data_list);
SAME(init_data_count);
SAME(content_protections);
SAME(content_protection_count);
SAME(delta_update);
SAME(delta_update_count);
SAME(_dom);
SAME(_dom_size);
SAME(version_form);
#undef SAME

_Static_assert(MOQ_MSF_CATALOG_SIZE_THROUGH_VERSION_FORM ==
               offsetof(OLD, version_form) + sizeof(moq_msf_version_form_t),
               "the MSF-01 extent is the old layout through version_form");
_Static_assert(MOQ_MSF_CATALOG_V0_SIZE == offsetof(OLD, version_form),
               "the first published layout ends at _dom_size");
_Static_assert(offsetof(moq_msf_catalog_t, _reserved_revision) ==
               MOQ_MSF_CATALOG_SIZE_THROUGH_VERSION_FORM,
               "the reserved slot covers version_form's tail padding");
_Static_assert(offsetof(moq_msf_catalog_t, revision) >= sizeof(OLD),
               "revision starts past the old sizeof");
_Static_assert(offsetof(moq_msf_catalog_t, revision) ==
               MOQ_MSF_CATALOG_SIZE_THROUGH_VERSION_FORM + 4,
               "revision follows the reserved slot");
_Static_assert(offsetof(moq_msf_catalog_t, _reserved_publish_tracks) ==
               offsetof(moq_msf_catalog_t, revision) + 4,
               "the second reserved slot follows revision");
_Static_assert(offsetof(moq_msf_catalog_t, publish_tracks) ==
               offsetof(moq_msf_catalog_t, _reserved_publish_tracks) + 4,
               "publish_tracks follows its reserved slot");
/* The block at revision ends the struct. Any tail padding after it (on
 * targets that align uint64_t to 8 with 4-byte size_t) lies past the full
 * extent; a later append covers it with a reserved slot first. */
_Static_assert(MOQ_MSF_CATALOG_SIZE_THROUGH_PUBLISH_TRACKS <=
               sizeof(moq_msf_catalog_t) &&
               sizeof(moq_msf_catalog_t) -
                   MOQ_MSF_CATALOG_SIZE_THROUGH_PUBLISH_TRACKS < sizeof(uint64_t),
               "the block at revision ends the struct");
#undef OLD

/* An extension entry stays a kind and one pointer, whatever kinds exist. */
_Static_assert(sizeof(moq_msf_track_ext_t) ==
               offsetof(moq_msf_track_ext_t, u) + sizeof(void *),
               "moq_msf_track_ext_t is one pointer wide past its kind");
_Static_assert(sizeof(((moq_msf_track_ext_t *)0)->u) == sizeof(void *),
               "the union holds pointers only");

/* The sized records. */
_Static_assert(offsetof(moq_msf_track_supplement_t, struct_size) == 0 &&
               sizeof(((moq_msf_track_supplement_t *)0)->struct_size) == 4,
               "the supplement leads with struct_size");
_Static_assert(MOQ_MSF_TRACK_SUPPLEMENT_V0_SIZE ==
               offsetof(moq_msf_track_supplement_t, malformed) + sizeof(uint32_t),
               "the supplement's first published layout ends with malformed");
_Static_assert(offsetof(moq_msf_validate_cfg_t, struct_size) == 0 &&
               MOQ_MSF_VALIDATE_CFG_V0_SIZE == sizeof(moq_msf_validate_cfg_t),
               "the validate cfg's first published layout is the whole struct");

/* Values that never renumber. */
_Static_assert(MOQ_MSF_REVISION_01 == 0 && MOQ_MSF_REVISION_02 == 1, "revisions");
_Static_assert(MOQ_MSF_EXT_MPEG2TS == 1 && MOQ_MSF_EXT_SUPPLEMENT == 2,
               "extension kinds");
_Static_assert(MOQ_MSF_DELTA_OP_ADD == 1 && MOQ_MSF_DELTA_OP_REMOVE == 2 &&
               MOQ_MSF_DELTA_OP_CLONE == 3 && MOQ_MSF_DELTA_OP_UPDATE == 4,
               "delta op kinds");
_Static_assert(MOQ_MSF_STREAM_MAPPING_GROUP == 0 &&
               MOQ_MSF_STREAM_MAPPING_OBJECT == 1 &&
               MOQ_MSF_STREAM_MAPPING_UNRECOGNIZED == 2, "stream mappings");
_Static_assert(MOQ_MSF_CATALOG_OK == 0 &&
               MOQ_MSF_CATALOG_ERR_DUPLICATE_TRACK == 1 &&
               MOQ_MSF_CATALOG_ERR_DUPLICATE_INIT_ID == 2 &&
               MOQ_MSF_CATALOG_ERR_DUPLICATE_CP_REF == 3 &&
               MOQ_MSF_CATALOG_ERR_DEPENDS_UNRESOLVED == 4 &&
               MOQ_MSF_CATALOG_ERR_DEPENDS_AMBIGUOUS == 5 &&
               MOQ_MSF_CATALOG_ERR_DUPLICATE_PUBLISH_TRACK == 6,
               "catalog errors");
_Static_assert(MOQ_MSF_SUPPLEMENT_F_NAMESPACE == 1u &&
               MOQ_MSF_SUPPLEMENT_F_PARENT_NAMESPACE == 2u, "malformed bits");
_Static_assert(MOQ_MSF_NAMESPACE_MAX_FIELDS == 32u, "namespace field limit");

int main(void)
{
    return 0;
}
