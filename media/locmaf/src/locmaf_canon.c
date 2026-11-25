/*
 * The canonical decisions (sections 11.1, 15.4, 15.6): which fields an
 * encoder emits and which box flags the writer sets, both from the same
 * effective values and track facts.
 */
#include "locmaf_internal.h"

void locmaf_decide(const moq_locmaf_track_t *track, uint32_t sdi,
                   const moq_locmaf_sample_t *samples, uint32_t n,
                   uint64_t payload_len, locmaf_canon_t *out)
{
    memset(out, 0, sizeof(*out));
    out->n = n;
    /* n = 0: fields 10 and 14 only; no optional tfhd field, no trun flag
     * beyond the data offset (sections 14, 15.6). */
    if (n == 0) return;

    out->sdi = sdi != track->trex_default_sample_description_index;

    bool dur_same = true, size_same = true, flags_same = true, rest_same = true;
    for (uint32_t i = 1; i < n; i++) {
        dur_same &= samples[i].duration == samples[0].duration;
        size_same &= samples[i].size == samples[0].size;
        flags_same &= samples[i].flags == samples[0].flags;
        if (i >= 2) rest_same &= samples[i].flags == samples[1].flags;
    }
    out->dur_list = !dur_same;
    out->dur_default = dur_same && samples[0].duration != track->trex_default_sample_duration;
    out->dur = samples[0].duration;

    /* Sizes (sections 11.2, 15.6.1): one sample is uniform with s = P. */
    uint32_t s = n == 1 ? (uint32_t)payload_len : samples[0].size;
    out->size_list = n > 1 && !size_same;
    out->size_wire = n > 1 && size_same && s != track->trex_default_sample_size;
    out->size_box = (n == 1 || size_same) && s != track->trex_default_sample_size;
    out->size = s;

    /* Flags: all equal, first-sample override, or per-sample. */
    out->first_flags = n > 1 && !flags_same && rest_same;
    out->flags_list = !flags_same && !out->first_flags;
    uint32_t v = out->first_flags ? samples[1].flags : samples[0].flags;
    out->flags_default = !out->flags_list && v != track->trex_default_sample_flags;
    out->flags = v;
    out->first = samples[0].flags;

    for (uint32_t i = 0; i < n; i++) {
        out->cto |= samples[i].composition_offset != 0;
        out->cto_signed |= samples[i].composition_offset < 0;
    }
}
