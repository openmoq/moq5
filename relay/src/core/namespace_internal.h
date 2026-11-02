#ifndef MOQR_RELAY_NAMESPACE_INTERNAL_H
#define MOQR_RELAY_NAMESPACE_INTERNAL_H

#include "relay_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

uint64_t *
cluster_path(const moqr_core_t *c, uint32_t ri);

bool
cluster_eligible(const moqr_core_t *c, uint32_t ri, uint32_t requester,
    uint32_t requester_gen, uint32_t excluded, uint32_t excluded_gen);

uint32_t
cluster_owner_node(const moqr_core_t *c, moqr_cluster_advertisement_id_t id,
                   moqr_ns_t ns);

void
cluster_project(moqr_core_t *c, uint32_t node, uint32_t ri);

uint32_t
cluster_longest_node(const moqr_core_t *c, moqr_ns_t ns);

uint32_t
cluster_select_node(moqr_core_t *c, moqr_ns_t ns, uint32_t requester,
    uint32_t requester_gen);

bool
cluster_route_warm(const moqr_core_t *c, uint32_t ri);

moqr_result_t
cluster_unannounce(moqr_core_t *c, uint32_t bs, uint32_t bg, uint32_t node,
    uint64_t lifetime);

void
cluster_pin_track(moqr_core_t *c, r_track_t *t);

bool
cluster_track_eligible(const moqr_core_t *c, const r_track_t *t,
    uint32_t requester, uint32_t requester_gen);

bool
cluster_should_reparent(moqr_ns_t ns, uint64_t local, uint64_t peer);

void
cluster_warm_flush(moqr_core_t *c);

uint32_t
cluster_failover_node(moqr_core_t *c, moqr_ns_t ns, const r_track_t *t,
    uint32_t excluded, uint32_t excluded_gen, uint64_t excluded_lifetime);

void
cluster_reparent_sweep(moqr_core_t *c);

uint32_t
cluster_track_lookup(moqr_core_t *c, moqr_ns_t ns, moq_bytes_t name,
    uint32_t requester, uint32_t requester_gen, uint64_t *hash_out);

void
discovery_mark_all(moqr_core_t *c);

void
tsub_free(moqr_core_t *c, uint32_t i);

void
discovery_suppress(moqr_core_t *c, uint32_t bslot, uint32_t bgen, uint32_t tslot);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_RELAY_NAMESPACE_INTERNAL_H */
