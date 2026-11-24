/* A program that needs every media parser. Built as C and as C++ from
 * `pkg-config --cflags --libs libmoq-media`. Silent on success.
 *
 * Each call below is defined in a different object of the tier, so each
 * archive must be on the link line, in an order that resolves it. */
#include <stdio.h>

#include <moq/cmaf.h>
#include <moq/loc.h>
#include <moq/media_object.h>
#include <moq/mpeg2ts.h>
#include <moq/msf.h>
#include <moq/msf_media.h>

#define CHECK(expr) do { if (!(expr)) { \
    fprintf(stderr, "tier consumer media:%d: %s\n", __LINE__, #expr); \
    return 1; \
} } while (0)

int main(void)
{
    moq_loc_headers_t loc;
    moq_cmaf_init_info_t init;
    moq_mpeg2ts_packet_t packet;
    moq_media_track_info_t track;
    moq_msf_mpeg2ts_t ts;

    moq_loc_headers_init(&loc);                              /* LOC */
    moq_cmaf_init_info_init_sized(&init, sizeof(init));      /* CMAF */
    moq_mpeg2ts_packet_init(&packet);                        /* MPEG-TS */
    /* The media object normalizer, which also holds the MPEG-TS parsing. */
    moq_media_track_info_init_sized(&track, sizeof(track));
    CHECK(track.struct_size == sizeof(track));
    moq_msf_mpeg2ts_init_sized(&ts, sizeof(ts));             /* MSF catalog */

    /* The catalog-to-media bridge is its own object in the MSF library and
     * calls into the media object normalizer, CMAF and LOC. It refuses a
     * missing track before anything else. */
    CHECK(moq_msf_track_to_media_info_sized(NULL, NULL, &track, sizeof(track),
                                            MOQ_VERSION_DRAFT_16, NULL, 0,
                                            NULL) == MOQ_ERR_INVAL);

    /* The MSF-02 catalog surface: each call refuses its missing arguments,
     * so no allocator is needed. */
    moq_msf_catalog_t cat;
    moq_msf_validate_cfg_t vcfg;
    moq_msf_track_supplement_t supp;
    moq_bytes_t fields[2];
    size_t nfields = 0;
    uint64_t value = 0;
    moq_bytes_t none = { NULL, 0 };
    moq_namespace_t no_ns = { NULL, 0 };
    moq_msf_validate_cfg_init(&vcfg);
    CHECK(vcfg.struct_size == sizeof(vcfg));
    moq_msf_track_supplement_init(&supp);
    CHECK(supp.struct_size == sizeof(supp));
    CHECK(moq_msf_catalog_parse_sized(NULL, none, &cat, sizeof(cat)) ==
          MOQ_ERR_INVAL);
    CHECK(moq_msf_catalog_apply_delta_sized(NULL, &cat, &cat, &vcfg, &cat,
                                            sizeof(cat)) == MOQ_ERR_INVAL);
    CHECK(moq_msf_catalog_check(NULL, &cat, &vcfg, NULL) == MOQ_ERR_INVAL);
    CHECK(moq_msf_track_supplement(NULL) == NULL);
    CHECK(moq_msf_ext_supplement(NULL, 0) == NULL);
    CHECK(moq_msf_namespace_split(none, fields, 2, &nfields) == MOQ_ERR_PROTO);
    CHECK(!moq_msf_track_matches(NULL, no_ns, no_ns, none));
    CHECK(!moq_msf_packaging_metadata_find(none, "streamMapping", &none));
    CHECK(moq_msf_track_stream_mapping(NULL) == MOQ_MSF_STREAM_MAPPING_GROUP);
    CHECK(moq_msf_switch_point_group(2, 0, 15, 30, &value) == MOQ_OK &&
          value == 1);
    CHECK(moq_msf_init_data_property_type(NULL, &value) == MOQ_ERR_INVAL);
    return 0;
}
