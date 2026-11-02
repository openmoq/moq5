#ifndef MOQR_RELAY_TRACKS_INTERNAL_H
#define MOQR_RELAY_TRACKS_INTERNAL_H

#include "relay_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

bool
ns_view_ok(moqr_ns_t ns, size_t min_count);

bool
ftn_view_ok(moqr_ns_t ns, moq_bytes_t name);

r_track_t *
track_resolve(moqr_core_t *c, moqr_track_t h, uint32_t *out_slot);

uint64_t
key_view_hash(moqr_ns_t ns, moq_bytes_t name);

bool
key_view_eq(const r_key_t *k, moqr_ns_t ns, moq_bytes_t name);

uint32_t
trie_find_child(const moqr_core_t *c, uint32_t node, moq_bytes_t part);

moqr_result_t
trie_child_get_or_create(moqr_core_t *c, uint32_t node, moq_bytes_t part,
                         uint32_t *out);

void
track_src_set(moqr_core_t *c, r_track_t *t, uint32_t node);

uint32_t
trie_reclaimable_count(const moqr_core_t *c);

uint32_t
trie_walk(const moqr_core_t *c, moqr_ns_t ns);

uint32_t
trie_longest_announce(const moqr_core_t *c, moqr_ns_t ns);

uint32_t
node_depth(const moqr_core_t *c, uint32_t node);

bool
prefix_matches_node(const moqr_core_t *c, const r_ns_sub_t *nsub,
                    uint32_t node);

uint32_t
track_lookup(const moqr_core_t *c, moqr_ns_t ns, moq_bytes_t name,
             uint64_t *hash_out);

void
track_free_slot(moqr_core_t *c, uint32_t slot);

void
track_touch(moqr_core_t *c, uint32_t slot);

bool
track_reclaimable(const moqr_core_t *c, uint32_t slot);

void
track_reclaim(moqr_core_t *c, uint32_t slot, uint32_t reason);

moqr_result_t
track_make_log(moqr_core_t *c, uint32_t slot, moqr_log_t **out);

void
fetch_retire(moqr_core_t *c, r_fetch_t *f);

void
track_fetch_invalidate(moqr_core_t *c, uint32_t track_slot,
                       uint32_t track_gen);

void
track_replace_retained_generation(moqr_core_t *c, uint32_t slot,
                                  moqr_log_t *new_log);

uint32_t
track_free_slot_find(const moqr_core_t *c);

moqr_result_t
r_room_check(const moqr_core_t *c, bool need_slot, uint32_t need_nodes,
             uint64_t need_bytes, uint32_t keep, moqr_refuse_reason_t *reason);

moqr_result_t
r_make_room(moqr_core_t *c, bool need_slot, uint32_t need_nodes,
            uint64_t need_bytes, uint32_t *keep_node);

moqr_result_t
track_create(moqr_core_t *c, moqr_ns_t ns, moq_bytes_t name, uint64_t hash,
             uint32_t *out_slot);

void
track_install(moqr_core_t *c, uint32_t slot, r_key_t key, uint64_t hash,
              moqr_log_t *log);

moqr_track_t
track_handle(const moqr_core_t *c, uint32_t slot);

moqr_sub_t
sub_handle(const moqr_core_t *c, uint32_t slot);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_RELAY_TRACKS_INTERNAL_H */
