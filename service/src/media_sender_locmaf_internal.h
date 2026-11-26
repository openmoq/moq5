#ifndef MOQ_MEDIA_SENDER_LOCMAF_INTERNAL_H
#define MOQ_MEDIA_SENDER_LOCMAF_INTERNAL_H

/*
 * The sender's LOCMAF packaging (draft-einarsson-moq-locmaf-01): each
 * track's encoder, and the wire buffer one queued Object is encoded into.
 *
 * write() takes one CMAF chunk and reserves, before the Object is queued,
 * the buffer its encoding needs (the encoder's bound): one allocation whose
 * start holds its own owner (the allocator and the size), so the buffer can
 * be released from the block alone, after the sender is gone, when the last
 * reference to the Object's payload goes. The drain encodes the chunk at
 * the Location it assigns into that buffer and wraps it; the encoder adopts
 * the Object as its reference only once the facade took the write.
 */

#include "media_sender_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

/* Release an unwrapped wire buffer through the owner at its start. NULL is a
 * no-op. */
void sender_lm_wire_free(void *block);

/* The track's encoder, from its CMAF Header: MOQ_ERR_INVAL for a header
 * LOCMAF cannot use, MOQ_ERR_NOMEM, MOQ_ERR_UNSUPPORTED without moq::locmaf.
 * Nothing is kept on failure. */
moq_result_t sender_lm_prepare(moq_media_sender_t *s, moq_media_track_t *t);
void sender_lm_free(moq_media_sender_t *s, moq_media_track_t *t);

/* write(), s->mu held: check the chunk against the encoder (MOQ_ERR_PROTO,
 * MOQ_ERR_UNSUPPORTED, MOQ_ERR_LIMIT, MOQ_ERR_INVAL) and reserve its wire
 * buffer (MOQ_ERR_NOMEM). On MOQ_OK *block (with *block_size, which the
 * queue charges, and *bound, its payload capacity) is the caller's. */
moq_result_t sender_lm_reserve(moq_media_sender_t *s, moq_media_track_t *t,
                               moq_bytes_t chunk, void **block,
                               size_t *block_size, size_t *bound);

/* The drain, s->mu held: the entry's wire payload for Location (group,
 * object), encoded and wrapped at the first attempt and kept by the entry
 * for a retry. MOQ_ERR_NOMEM: the wrap failed and the entry keeps its
 * buffer, nothing committed (retry later); any other error is the
 * encoder's refusal at emit. */
moq_result_t sender_lm_emit(moq_media_sender_t *s, moq_media_track_t *t,
                            sender_preq_entry_t *e, uint64_t group,
                            uint64_t object, moq_rcbuf_t **wire);

/* The facade took the entry's write: the encoder adopts it. */
void sender_lm_commit(moq_media_track_t *t);

/* The track's open Group was abandoned on the wire: the next Object is a
 * full header. */
void sender_lm_reset(moq_media_track_t *t);

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQ_MEDIA_SENDER_LOCMAF_INTERNAL_H */
