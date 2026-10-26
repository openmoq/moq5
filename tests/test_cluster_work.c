#include "../core/src/wire/params_d22_internal.h"
#include <stdio.h>
static size_t reads;
static moq_result_t counted(moq_buf_reader_t *r, uint64_t *out)
{
    ++reads;
    return moq_buf_read_vi64(r, out);
}
#define moq_buf_read_vi64 counted
#include "../core/src/wire/params_d22.c"
#undef moq_buf_read_vi64
int main(void)
{
    uint8_t path[16384];
    moq_buf_writer_t w = {path, sizeof path, 0};
    for (uint64_t id = 1; id <= 4096; id++) {
        if (moq_buf_write_vi64(&w, id) != MOQ_OK)
            return 1;
    }
    if (moq_d22_validate_hop_path((moq_bytes_t){path, w.pos}) != MOQ_OK)
        return 1;
    if (reads > 4096 * 128) {
        fprintf(stderr, "path validation used %zu primitive reads for 4096 IDs\n", reads);
        return 1;
    }
    if (moq_buf_write_vi64(&w, 2048) != MOQ_OK)
        return 1;
    if (moq_d22_validate_hop_path((moq_bytes_t){path, w.pos}) != MOQ_ERR_PROTO)
        return 1;
    return 0;
}
