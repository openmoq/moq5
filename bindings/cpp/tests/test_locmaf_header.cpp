// <moq/locmaf.h> stands alone in C++: it is this translation unit's first
// include, compiled at C++17 without the C++ binding.
#include <moq/locmaf.h>

#include <cstdio>

int main()
{
    int failures = 0;

    moq_locmaf_track_t track;
    moq_locmaf_track_init(&track);
    if (track.struct_size != sizeof(track)) failures++;

    moq_locmaf_decoder_cfg_t dcfg;
    moq_locmaf_decoder_cfg_init(&dcfg);
    size_t dbytes = 0;
    if (moq_locmaf_decoder_storage_size(&dcfg, &dbytes) != MOQ_OK || dbytes == 0) failures++;

    moq_locmaf_encoder_cfg_t ecfg;
    moq_locmaf_encoder_cfg_init(&ecfg);
    size_t ebytes = 0;
    if (moq_locmaf_encoder_storage_size(&ecfg, &ebytes) != MOQ_OK || ebytes == 0) failures++;

    if (moq_locmaf_reason_name(MOQ_LOCMAF_REASON_NO_REFERENCE)[0] == '\0') failures++;

    std::printf("%s: cpp_locmaf_header: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
