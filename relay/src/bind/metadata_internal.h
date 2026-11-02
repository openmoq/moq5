#ifndef MOQR_BIND_METADATA_INTERNAL_H
#define MOQR_BIND_METADATA_INTERNAL_H

#include "bind_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

void
bind_meta_sweep(moqr_bind_t *b);

moqr_result_t
bind_meta_copy(moqr_bind_t *b, const moqr_bind_meta_ref_t *input,
               moqr_bind_meta_ref_t *out);

bool
bind_meta_publish_count(const moqr_core_limits_t *lim,
                        const moqr_bind_limits_t *bl, uint32_t *out);

b_meta_publish_t *
bind_meta_pub_find(moqr_bind_t *b, const b_conn_t *cn, uint64_t pub);

b_meta_publish_t *
bind_meta_pub_reserve(moqr_bind_t *b, const b_conn_t *cn, uint64_t pub);

void
bind_meta_pub_release(moqr_bind_t *b, b_meta_publish_t *p);

void
bind_meta_pub_refuse_ex(moqr_bind_t *b, b_conn_t *cn, b_meta_publish_t *p,
                        uint64_t code, bool can_retry, uint64_t retry_ms,
                        uint64_t now_us);

void
bind_meta_pub_refuse(moqr_bind_t *b, b_conn_t *cn, b_meta_publish_t *p,
                     uint64_t code, uint64_t now_us);

void
bind_meta_pub_purge(moqr_bind_t *b, const b_conn_t *cn);

moqr_bind_meta_ref_t
bind_meta_input(const b_conn_t *cn, uint64_t handle, uint64_t gen,
                moq_bytes_t bytes);

void
bind_track_props_release(moqr_bind_t *b, b_props_tmp_t *tmp);

bool
bind_track_props_out(moqr_bind_t *b, uint8_t src, uint8_t dst, moq_bytes_t in,
                     moq_bytes_t *out, b_props_tmp_t *tmp);

bool
bind_meta_out(moqr_bind_t *b, const moqr_bind_meta_ref_t *pin, moq_version_t version,
              moq_bytes_t *out, b_props_tmp_t *tmp);

void
bind_meta_reply_pin(moqr_bind_t *b, moqr_track_t track, moqr_bind_meta_ref_t *pin);

bool
bind_fetch_ok_pin(moqr_bind_t *b, const b_conn_t *cn, moqr_fetch_t cfetch,
                  moqr_bind_meta_ref_t *pin, moq_bytes_t *out, b_props_tmp_t *tmp);

void
bind_meta_prepare_reply(moqr_bind_t *b, moqr_intent_t *it,
                        moqr_bind_meta_ref_t *pin);

bool
bind_try_pinned(moqr_bind_t *b, const moqr_intent_t *it, uint64_t now_us,
                uint8_t mode, bool *parked, moqr_intent_t *defer,
                const moqr_bind_meta_ref_t *pin);

moq_result_t
bind_props_out(moqr_bind_t *b, const b_conn_t *cn, const moq_rcbuf_t *props,
               uint8_t codec, moq_rcbuf_t **out, moq_rcbuf_t **owned);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_BIND_METADATA_INTERNAL_H */
