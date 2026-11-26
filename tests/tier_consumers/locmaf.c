/* A program that needs the LOCMAF encoder and decoder. Built as C and as
 * C++ from `pkg-config --cflags --libs libmoq-locmaf`. Silent on success.
 *
 * The calls below reach the LOCMAF library, the CMAF parser it takes its
 * track facts from, and the core, so each archive must be on the link line,
 * in an order that resolves it. */
#include <stdio.h>
#include <stdlib.h>

#include <moq/cmaf.h>
#include <moq/locmaf.h>
#include <moq/types.h>

#define CHECK(expr) do { if (!(expr)) { \
    fprintf(stderr, "tier consumer locmaf:%d: %s\n", __LINE__, #expr); \
    return 1; \
} } while (0)

int main(void)
{
    moq_cmaf_init_info_t init;
    moq_locmaf_track_t track;
    moq_locmaf_decoder_cfg_t cfg;
    moq_locmaf_decoder_t dec;
    moq_locmaf_chunk_t chunk;
    moq_locmaf_reason_t why = MOQ_LOCMAF_REASON_NONE;
    moq_bytes_t empty = { NULL, 0 };
    size_t bytes = 0;
    void *storage;
    moq_result_t rc;

    CHECK(moq_alloc_default() != NULL);                          /* core */
    moq_cmaf_init_info_init_sized(&init, sizeof(init));          /* CMAF */
    /* A header without a trak is not one LOCMAF can use. */
    CHECK(moq_locmaf_track_from_cmaf(&init, &track, sizeof(track)) ==
          MOQ_ERR_PROTO);

    moq_locmaf_track_init(&track);
    track.track_id = 1;
    track.trex_default_sample_description_index = 1;
    moq_locmaf_decoder_cfg_init(&cfg);
    cfg.track = &track;
    CHECK(moq_locmaf_decoder_storage_size(&cfg, &bytes) == MOQ_OK && bytes > 0);
    storage = malloc(bytes);
    CHECK(storage != NULL);
    cfg.storage = storage;
    cfg.storage_size = bytes;
    CHECK(moq_locmaf_decoder_init(&dec, sizeof(dec), &cfg) == MOQ_OK);
    /* An Object with no element is refused, with its reason. */
    rc = moq_locmaf_decoder_push(&dec, 0, 0, empty, &chunk, sizeof(chunk),
                                 &why);
    free(storage);
    CHECK(rc == MOQ_ERR_PROTO);
    CHECK(why == MOQ_LOCMAF_REASON_EMPTY_OBJECT);
    CHECK(moq_locmaf_reason_name(why)[0] != '\0');
    return 0;
}
