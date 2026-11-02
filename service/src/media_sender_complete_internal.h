#ifndef MOQ_MEDIA_SENDER_COMPLETE_INTERNAL_H
#define MOQ_MEDIA_SENDER_COMPLETE_INTERNAL_H

#include "media_sender_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

void sender_note_completion_failure(moq_media_sender_t *s, moq_result_t rc);

bool track_is_generated_timeline(const moq_media_track_t *t);

bool sender_terminal_admitted(const moq_media_sender_t *s,
                              const moq_media_track_t *t);

bool sender_timeline_caught_up(const moq_media_track_t *tl);

bool sender_timeline_media_ended(const moq_media_sender_t *s,
                                 const moq_media_track_t *tl);

void sender_drain_completion(moq_media_sender_t *s, uint64_t now_us);

bool sender_finish_conversions(moq_media_sender_t *s, uint64_t now_us);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQ_MEDIA_SENDER_COMPLETE_INTERNAL_H */
