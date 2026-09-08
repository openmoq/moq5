#include <moq/relay/relay.h>
#include <string.h>

moqr_result_t
moqr_core_get_stats_sized(const moqr_core_t *core, moqr_core_stats_t *out,
                          size_t out_size)
{
    if (core == NULL || out == NULL || out_size < sizeof(*out)) {
        return MOQR_ERR_INVAL;
    }
    moqr_core_stats_t sample;
    moqr_core_get_stats(core, &sample);
    memcpy(out, &sample, sizeof(sample));
    return MOQR_OK;
}

moqr_result_t
moqr_log_get_stats_sized(const moqr_log_t *log, moqr_log_stats_t *out,
                         size_t out_size)
{
    if (log == NULL || out == NULL || out_size < sizeof(*out)) {
        return MOQR_ERR_INVAL;
    }
    moqr_log_stats_t sample;
    moqr_log_get_stats(log, &sample);
    memcpy(out, &sample, sizeof(sample));
    return MOQR_OK;
}
