#ifndef MOQR_BIND_UPSTREAM_INTERNAL_H
#define MOQR_BIND_UPSTREAM_INTERNAL_H

#include "bind_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

void
bind_upstream_terminated(moqr_bind_t *b, b_conn_t *cn,
                         moq_request_family_t family, uint64_t handle_raw,
                         bind_upstream_end_t how, uint64_t now_us);

moqr_track_t
bind_ingest_track(moqr_bind_t *b, b_conn_t *cn, moq_subscription_t sub,
                  moq_publication_t pub);

bool
bind_terminal_owns_track(moqr_bind_t *b, moqr_track_t track, uint64_t source_gen);

uint32_t
bind_ingest_refused(moqr_bind_t *b, b_conn_t *cn, moqr_track_t track,
                    const moqr_log_append_desc_t *d, moqr_result_t rc,
                    moq_subscription_t sub, moq_publication_t pub,
                    uint64_t now_us);

uint32_t
bind_conn_slot(const moqr_bind_t *b, const b_conn_t *cn);

b_dup_owner_t *
bind_dup_owner_find(moqr_bind_t *b, const b_conn_t *cn, uint64_t src,
                    const moq_object_chunk_event_t *oc);

void
bind_dup_owner_chunk(moqr_bind_t *b, b_conn_t *cn, b_dup_owner_t *o,
                     const moq_object_chunk_event_t *oc, uint64_t now_us);

void
bind_dup_owners_release(moqr_bind_t *b, const b_conn_t *cn);

void
usub_store_key(moqr_bind_t *b, b_usub_t *us, const moq_bytes_t *parts,
               uint32_t ns_count, moq_bytes_t name);

void
usub_release(moqr_bind_t *b, b_usub_t *us);

void
usub_release_all(moqr_bind_t *b, b_conn_t *cn);

void
bind_reset_track_streams(moqr_bind_t *b, moqr_track_t track, uint64_t now_us);

void
bind_fetches_malformed(moqr_bind_t *b, moqr_track_t track, uint64_t now_us);

bool
bind_cancel_settled(moqr_bind_t *b, b_conn_t *cn, moq_result_t rc, const char *what,
                    uint64_t now_us);

moq_result_t
bind_cancel_upstream_sub(b_conn_t *cn, moq_subscription_t ssub, uint64_t now_us);

moq_result_t
bind_cancel_upstream_pub(b_conn_t *cn, moq_publication_t pub, uint64_t code,
                         uint64_t now_us);

void
bind_retry_owed_cancels(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_BIND_UPSTREAM_INTERNAL_H */
