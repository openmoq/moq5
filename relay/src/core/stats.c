#include "relay_internal.h"
#include "tracks_internal.h"

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

/* -- stats ------------------------------------------------------------------------------------------------- */

void
moqr_core_get_stats(const moqr_core_t *c, moqr_core_stats_t *out)
{
    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (c == NULL) {
        return;
    }
    for (uint32_t i = 0; i < c->max_bindings; i++) {
        if ((c->bindings[i].gen & 1u) != 0) {
            out->bindings++;
        }
    }
    for (uint32_t i = 0; i < c->max_subs; i++) {
        if ((c->subs[i].gen & 1u) != 0) {
            out->subs++;
            if (c->subs[i].state == R_SUB_PARKED) {
                out->subs_parked++;
            } else if (c->subs[i].state == R_SUB_ACTIVE) {
                out->subs_active++;
            }
        }
    }
    out->evicted_total = c->evicted_freed;
    out->tracks_evicted_capacity = c->tracks_evicted_capacity;
    out->tracks_evicted_idle = c->tracks_evicted_idle;
    out->ns_nodes_reclaimed = c->ns_nodes_reclaimed;
    for (uint32_t i = 0; i < c->max_tracks; i++) {
        if ((c->tracks[i].gen & 1u) != 0) {
            out->tracks++;
            if (track_reclaimable(c, i)) {
                out->tracks_reclaimable++;
            }
            moqr_log_stats_t ls;
            moqr_log_get_stats(c->tracks[i].log, &ls);
            out->retained_bytes += ls.retained_bytes;
            out->evicted_total += ls.evicted_records_total;
        }
    }
    for (uint32_t i = 0; i < c->max_ns_subs; i++) {
        if (c->ns_subs[i].used) {
            out->ns_subs++;
        }
    }
    out->ns_nodes = c->node_count;
    out->ns_nodes_reclaimable = trie_reclaimable_count(c);
    out->ingested_total = c->ingested_total;
    out->delivered_total = c->delivered_total;
    out->evict_sweeps = c->evict_sweeps;
    out->note_emitted_total = c->note_emitted_total;
    out->intent_highwater = c->intent_hwm;
    out->route_epoch = c->route_epoch;
    for (uint32_t i = 0; i < MOQR_REFUSE__COUNT; i++) {
        out->refusals[i] = c->refusals[i];
    }
    for (uint32_t act = 0; act < MOQR_AUTH_ACTION__COUNT; act++) {
        for (uint32_t dec = 0; dec < 3; dec++) {
            out->auth_decisions[act][dec] = c->auth_decisions[act][dec];
        }
    }
    for (uint32_t rz = 0; rz < MOQR_AUTH_REASON__COUNT; rz++) {
        out->auth_denials[rz] = c->auth_denials[rz];
    }
}


const char *
moqr_refuse_reason_name(moqr_refuse_reason_t reason)
{
    switch (reason) {
    case MOQR_REFUSE_TRACKS:     return "tracks";
    case MOQR_REFUSE_SUBS:       return "subs";
    case MOQR_REFUSE_NS_NODES:   return "ns_nodes";
    case MOQR_REFUSE_NS_SUBS:    return "ns_subs";
    case MOQR_REFUSE_BINDINGS:   return "bindings";
    case MOQR_REFUSE_NAME_BYTES: return "name_bytes";
    case MOQR_REFUSE_FETCHES:    return "fetches";
    default:                     return "unknown";
    }
}


const char *
moqr_auth_action_name(moqr_auth_action_t action)
{
    switch (action) {
    case MOQR_AUTH_CLIENT_SETUP:        return "client_setup";
    case MOQR_AUTH_SERVER_SETUP:        return "server_setup";
    case MOQR_AUTH_PUBLISH_NAMESPACE:   return "publish_namespace";
    case MOQR_AUTH_SUBSCRIBE_NAMESPACE: return "subscribe_namespace";
    case MOQR_AUTH_SUBSCRIBE:           return "subscribe";
    case MOQR_AUTH_REQUEST_UPDATE:      return "request_update";
    case MOQR_AUTH_PUBLISH:             return "publish";
    case MOQR_AUTH_FETCH:               return "fetch";
    case MOQR_AUTH_TRACK_STATUS:        return "track_status";
    default:                           return "unknown";
    }
}


const char *
moqr_auth_reason_name(moqr_auth_reason_t reason)
{
    switch (reason) {
    case MOQR_AUTH_REASON_OK:            return "ok";
    case MOQR_AUTH_REASON_NO_TOKEN:      return "no_token";
    case MOQR_AUTH_REASON_UNSCOPED:      return "unscoped";
    case MOQR_AUTH_REASON_EXPIRED:       return "expired";
    case MOQR_AUTH_REASON_NOT_YET_VALID: return "not_yet_valid";
    case MOQR_AUTH_REASON_BAD_ISSUER:    return "bad_issuer";
    case MOQR_AUTH_REASON_BAD_AUDIENCE:  return "bad_audience";
    case MOQR_AUTH_REASON_BAD_SIGNATURE: return "bad_signature";
    case MOQR_AUTH_REASON_POLICY:        return "policy";
    default:                            return "unknown";
    }
}
