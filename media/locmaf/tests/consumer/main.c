/* An installed moq::locmaf, found with find_package(libmoq): encode one CMAF
 * chunk as a LOCMAF Object, decode it, and write the canonical chunk back,
 * byte for byte. Built as C and as C++. */
#include <moq/locmaf.h>
#include <stdlib.h>
#include <string.h>

int main(void)
{
    static const uint8_t payload[4] = { 1, 2, 3, 4 };
    moq_locmaf_track_t track;
    moq_locmaf_track_init(&track);
    track.track_id = 1;
    track.trex_default_sample_description_index = 1;
    track.trex_default_sample_duration = 1024;

    moq_locmaf_sample_t s;
    memset(&s, 0, sizeof(s));
    s.duration = 1024;
    s.size = sizeof(payload);
    s.flags = 0x02000000u;
    moq_locmaf_chunk_t c;
    memset(&c, 0, sizeof(c));
    c.struct_size = sizeof(c);
    c.kind = MOQ_LOCMAF_OBJECT_FULL;
    c.sample_description_index = 1;
    c.sample_count = 1;
    c.samples = &s;
    c.mdat_payload.data = payload;
    c.mdat_payload.len = sizeof(payload);
    uint8_t cmaf[256];
    size_t cmaf_len = 0;
    if (moq_locmaf_write_cmaf(&track, &c, cmaf, sizeof(cmaf), &cmaf_len) != MOQ_OK)
        return 1;

    moq_locmaf_encoder_cfg_t ecfg;
    moq_locmaf_encoder_cfg_init(&ecfg);
    ecfg.track = &track;
    size_t ebytes = 0;
    if (moq_locmaf_encoder_storage_size(&ecfg, &ebytes) != MOQ_OK) return 1;
    moq_locmaf_decoder_cfg_t dcfg;
    moq_locmaf_decoder_cfg_init(&dcfg);
    dcfg.track = &track;
    size_t dbytes = 0;
    if (moq_locmaf_decoder_storage_size(&dcfg, &dbytes) != MOQ_OK) return 1;
    void *emem = malloc(ebytes);
    void *dmem = malloc(dbytes);
    int rc = 1;
    moq_locmaf_encoder_t e;
    moq_locmaf_decoder_t d;
    uint8_t obj[256], back[256];
    size_t obj_len = 0, back_len = 0;
    moq_locmaf_chunk_t out;
    moq_bytes_t in, wire;
    ecfg.storage = emem;
    ecfg.storage_size = ebytes;
    dcfg.storage = dmem;
    dcfg.storage_size = dbytes;
    in.data = cmaf;
    in.len = cmaf_len;
    if (!emem || !dmem ||
        moq_locmaf_encoder_init(&e, sizeof(e), &ecfg) != MOQ_OK ||
        moq_locmaf_decoder_init(&d, sizeof(d), &dcfg) != MOQ_OK ||
        moq_locmaf_encode_chunk(&e, 0, 0, in, obj, sizeof(obj), &obj_len,
                                NULL) != MOQ_OK)
        goto done;
    moq_locmaf_encoder_commit(&e);
    wire.data = obj;
    wire.len = obj_len;
    if (moq_locmaf_decoder_push(&d, 0, 0, wire, &out, sizeof(out), NULL) != MOQ_OK ||
        moq_locmaf_write_cmaf(&track, &out, back, sizeof(back), &back_len) != MOQ_OK)
        goto done;
    rc = back_len == cmaf_len && memcmp(back, cmaf, cmaf_len) == 0 &&
         obj_len < cmaf_len ? 0 : 1;
done:
    free(emem);
    free(dmem);
    return rc;
}
