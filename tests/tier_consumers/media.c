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
    return 0;
}
