#ifndef MOQ_MEDIA_SENDER_WRITE_INTERNAL_H
#define MOQ_MEDIA_SENDER_WRITE_INTERNAL_H

#include "media_sender_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

void preq_entry_release(moq_media_sender_t *s, sender_preq_entry_t *e);

void preq_note_dropped(moq_media_sender_t *s, moq_media_track_t *t,
                       const sender_preq_entry_t *e);

void preq_note_enqueued(moq_media_sender_t *s,
                        const sender_preq_entry_t *e);

uint32_t active_obj_cap(const moq_media_sender_t *s);

bool sender_holds_group(const moq_media_track_t *t, uint64_t g);

void preq_evict_group(moq_media_sender_t *s,
                      moq_media_track_t *t, uint64_t g);

moq_result_t sender_flush_reset(moq_media_sender_t *s,
                                moq_media_track_t *t, uint64_t now_us);

void sender_drain(moq_media_sender_t *s, uint64_t now_us);

bool sender_sum_fits(size_t *acc, size_t add);

moq_result_t sender_admit_locked(moq_media_sender_t *s,
                                 moq_media_track_t *track,
                                 const sender_admit_t *a);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQ_MEDIA_SENDER_WRITE_INTERNAL_H */
