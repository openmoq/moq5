#include <moq/wire.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    uint64_t value = 0;
    size_t n = moq_quic_varint_decode(data, size, &value);

    if (n > 0) {
        /* Roundtrip: re-encode with minimum encoding and verify. */
        uint8_t buf[8];
        size_t enc = moq_quic_varint_encode(value, buf, sizeof(buf));
        if (enc == 0)
            abort(); /* valid decode must produce encodable value */

        uint64_t check = 0;
        size_t dec = moq_quic_varint_decode(buf, enc, &check);
        if (dec == 0 || check != value)
            abort(); /* roundtrip mismatch */
    }

    return 0;
}
