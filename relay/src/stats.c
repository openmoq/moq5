#include <moq/relay/moqr_shards.h>
#include <string.h>

moqr_result_t
moqr_bind_get_stats_sized(const moqr_bind_t *bind, moqr_bind_stats_t *out,
                          size_t out_size)
{
    if (bind == NULL || out == NULL || out_size < sizeof(*out)) {
        return MOQR_ERR_INVAL;
    }
    moqr_bind_stats_t sample;
    moqr_bind_get_stats(bind, &sample);
    memcpy(out, &sample, sizeof(sample));
    return MOQR_OK;
}

moqr_result_t
moqr_shards_get_stats_sized(moqr_shards_t *runtime, uint16_t shard,
                           moqr_shards_stats_t *out, size_t out_size)
{
    if (runtime == NULL || out == NULL || out_size < sizeof(*out)) {
        return MOQR_ERR_INVAL;
    }
    moqr_shards_stats_t sample;
    moqr_result_t rc = moqr_shards_get_stats(runtime, shard, &sample);
    if (rc != MOQR_OK) {
        return rc;
    }
    memcpy(out, &sample, sizeof(sample));
    return MOQR_OK;
}
