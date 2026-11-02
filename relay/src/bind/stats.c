#include "bind_internal.h"

/* Fixed forward-latency bucket bounds (microseconds), 100us..1s. Shared by
 * the observer here and the exporter (via moqr_latency_bound_us). */
static const uint64_t moqr_lat_bounds[MOQR_LATENCY_BUCKETS] = {
    100u,   250u,   500u,    1000u,   2500u,   5000u,
    10000u, 25000u, 50000u,  100000u, 250000u, 1000000u,
};


uint64_t
moqr_latency_bound_us(uint32_t i)
{
    return i < MOQR_LATENCY_BUCKETS ? moqr_lat_bounds[i] : 0u;
}


void
moqr_latency_observe(moqr_latency_hist_t *h, uint64_t latency_us)
{
    uint32_t i = 0;
    while (i < MOQR_LATENCY_BUCKETS && latency_us > moqr_lat_bounds[i]) {
        i++;
    }
    h->bucket[i]++;   /* i == MOQR_LATENCY_BUCKETS => +Inf overflow */
    h->sum_us += latency_us;
    h->count++;
}


void
moqr_bind_get_stats(const moqr_bind_t *b, moqr_bind_stats_t *out)
{
    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (b == NULL) {
        return;
    }
    for (uint32_t i = 0; i < b->max_conns; i++) {
        if (b->conns[i].used) {
            out->conns++;
        }
    }
    out->events_translated = b->events_translated;
    out->deliveries_written = b->deliveries_written;
    out->ingest_refusals = b->ingest_refusals;
    out->session_errors = b->session_errors;
    out->forward_latency = b->forward_latency;
    out->pending_high_water = b->pending_high_water;
    out->pending_nonscalar_blocked = b->pending_nonscalar_blocked;
    out->nsu_high_water = b->nsu_high_water;
    out->nsu_live = b->nsu_live;
    out->nsu_retained = b->nsu_retained;
    out->nsu_failed_closed = b->nsu_failed_closed;
    out->ordered_failed_closed = b->ordered_failed_closed;
}
