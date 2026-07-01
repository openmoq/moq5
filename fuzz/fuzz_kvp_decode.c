#include <moq/kvp.h>
#include <stdint.h>
#include <stddef.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    moq_kvp_decoder_t dec;
    moq_kvp_decoder_init(&dec, data, size);

    moq_kvp_entry_t entry;
    int count = 0;

    while (count < 1024) {
        moq_result_t rc = moq_kvp_decode_next(&dec, &entry);
        if (rc == MOQ_DONE)
            break;
        if (rc < 0)
            break;
        count++;
    }

    return 0;
}
